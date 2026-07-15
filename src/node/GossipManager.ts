import dgram from "node:dgram";
import type { NodeInfo } from "../types/index.js";

export type MemberState = "ALIVE" | "SUSPECT" | "DEAD";

export interface MemberInfo {
    state: MemberState;
    /** Date.now() timestamp of last ALIVE signal */
    lastSeen: number;
    /** Date.now() when node entered SUSPECT state */
    suspectSince?: number;
}

export interface GossipMessage {
    type: "PING" | "PONG" | "PING-REQ" | "PONG-INDIRECT" | "MEMBERSHIP";
    from: string;
    target?: string;
    members?: Record<string, MemberState>;
}

export class GossipManager {
    private readonly selfNodeId: string;
    private readonly nodes: NodeInfo[];
    private readonly onNodeDead: (nodeId: string) => void;
    private readonly onNodeAlive: (nodeId: string) => void;
    
    private readonly pingInterval: number;
    private readonly suspectTimeout: number;
    private readonly pingTimeout: number;

    private readonly members: Map<string, MemberInfo>;
    private udpSocket: dgram.Socket | null = null;
    private gossipInterval: NodeJS.Timeout | null = null;

    /** Map of targetNodeId -> callback to resolve pending direct pings */
    private readonly pendingPongs: Map<string, () => void> = new Map();
    /** Map of "helperId:targetId" -> callback to resolve pending indirect pings */
    private readonly pendingIndirectPongs: Map<string, () => void> = new Map();

    /**
     * Constructs a GossipManager to manage node failure detection.
     *
     * @param selfNodeId - ID of the local node.
     * @param nodes - NodeInfo configurations for the entire cluster.
     * @param onNodeDead - Callback invoked when a node transitions to DEAD.
     * @param onNodeAlive - Callback invoked when a DEAD node recovers to ALIVE.
     * @param pingInterval - Time interval (ms) between gossip rounds. Default 2000.
     * @param suspectTimeout - Time (ms) a node can stay SUSPECT before being declared DEAD. Default 6000.
     * @param pingTimeout - Timeout (ms) for direct or indirect pings. Default 2000.
     */
    constructor(
        selfNodeId: string,
        nodes: NodeInfo[],
        onNodeDead: (nodeId: string) => void,
        onNodeAlive: (nodeId: string) => void,
        pingInterval: number = 2000,
        suspectTimeout: number = 6000,
        pingTimeout: number = 2000
    ) {
        this.selfNodeId = selfNodeId;
        this.nodes = nodes;
        this.onNodeDead = onNodeDead;
        this.onNodeAlive = onNodeAlive;
        this.pingInterval = pingInterval;
        this.suspectTimeout = suspectTimeout;
        this.pingTimeout = pingTimeout;

        this.members = new Map();
        // Initialize all peers as ALIVE at startup
        for (const node of nodes) {
            if (node.nodeId !== selfNodeId) {
                this.members.set(node.nodeId, {
                    state: "ALIVE",
                    lastSeen: Date.now()
                });
            }
        }
    }

    /**
     * Starts the UDP gossip server and begins the periodic gossip execution.
     * Sets up the listening UDP port and the interval running runGossipRound().
     */
    start(): void {
        if (this.udpSocket) return;

        this.udpSocket = dgram.createSocket("udp4");

        this.udpSocket.on("message", (msg, rinfo) => {
            try {
                this.handleMessage(msg, rinfo);
            } catch (err) {
                console.error(`[Gossip] Error handling UDP message on ${this.selfNodeId}:`, err);
            }
        });

        this.udpSocket.on("error", (err) => {
            console.error(`[Gossip] Socket error on ${this.selfNodeId}:`, err);
        });

        const selfInfo = this.nodes.find((n) => n.nodeId === this.selfNodeId);
        if (!selfInfo) {
            throw new Error(`Self node info not found for ${this.selfNodeId}`);
        }

        const selfPort = selfInfo.port + 1000;
        this.udpSocket.bind(selfPort, "0.0.0.0");

        this.gossipInterval = setInterval(() => {
            void this.runGossipRound();
        }, this.pingInterval);
    }

    /**
     * Stops the UDP gossip server, cancels the gossip interval, and cleans up all active timeouts.
     */
    stop(): void {
        if (this.gossipInterval) {
            clearInterval(this.gossipInterval);
            this.gossipInterval = null;
        }

        for (const resolve of this.pendingPongs.values()) {
            resolve();
        }
        this.pendingPongs.clear();

        for (const resolve of this.pendingIndirectPongs.values()) {
            resolve();
        }
        this.pendingIndirectPongs.clear();

        if (this.udpSocket) {
            try {
                this.udpSocket.close();
            } catch (err) {
                // Ignore
            }
            this.udpSocket = null;
        }
    }

    /**
     * Returns the membership state of a specific node.
     *
     * @param nodeId - The node ID to query.
     */
    getMemberState(nodeId: string): MemberState {
        if (nodeId === this.selfNodeId) return "ALIVE";
        return this.members.get(nodeId)?.state ?? "DEAD";
    }

    /**
     * Returns a copy of the current membership states of all cluster nodes.
     */
    getAllMembers(): Map<string, MemberInfo> {
        const copy = new Map(this.members);
        copy.set(this.selfNodeId, {
            state: "ALIVE",
            lastSeen: Date.now()
        });
        return copy;
    }

    /**
     * Returns true if the node is considered reachable (ALIVE or SUSPECT).
     *
     * @param nodeId - The node ID to check.
     */
    isAlive(nodeId: string): boolean {
        if (nodeId === this.selfNodeId) return true;
        const info = this.members.get(nodeId);
        return info ? info.state !== "DEAD" : true;
    }

    /**
     * Periodic routine to check SUSPECT nodes and initiate pings.
     * Implements SWIM protocol step 1 (SUSPECT promotion to DEAD) and step 2 (gossip ping sequence).
     */
    private async runGossipRound(): Promise<void> {
        // 1. Check suspect nodes
        const now = Date.now();
        for (const [nodeId, info] of this.members.entries()) {
            if (info.state === "SUSPECT" && info.suspectSince && now - info.suspectSince >= this.suspectTimeout) {
                info.state = "DEAD";
                info.suspectSince = undefined;
                this.onNodeDead(nodeId);
            }
        }

        // 2. Pick one random peer (ALIVE or SUSPECT only)
        const peers = Array.from(this.members.entries())
            .filter(([_, info]) => info.state !== "DEAD");

        if (peers.length === 0) return;

        const [targetId] = peers[Math.floor(Math.random() * peers.length)];

        // Direct PING
        const pongReceived = await this.sendPingAndWait(targetId);
        if (pongReceived) {
            return;
        }

        // Indirect PING (PING-REQ)
        const helpers = Array.from(this.members.entries())
            .filter(([id, info]) => id !== targetId && info.state !== "DEAD");

        if (helpers.length === 0) {
            // No helper available, transition to suspect directly
            this.markSuspect(targetId);
            return;
        }

        const [helperId] = helpers[Math.floor(Math.random() * helpers.length)];
        const indirectPongReceived = await this.sendPingReqAndWait(helperId, targetId);
        if (!indirectPongReceived) {
            this.markSuspect(targetId);
        }
    }

    /**
     * Sends a direct PING message to the target node and waits for a PONG response.
     *
     * @param targetId - Node ID to ping.
     */
    private sendPingAndWait(targetId: string): Promise<boolean> {
        return new Promise((resolve) => {
            const timeoutId = setTimeout(() => {
                this.pendingPongs.delete(targetId);
                resolve(false);
            }, this.pingTimeout);

            this.pendingPongs.set(targetId, () => {
                clearTimeout(timeoutId);
                this.pendingPongs.delete(targetId);
                resolve(true);
            });

            this.sendUdpMessage(targetId, {
                type: "PING",
                from: this.selfNodeId,
                members: this.getMembershipView()
            });
        });
    }

    /**
     * Requests a helper peer to ping the suspect node on our behalf.
     *
     * @param helperId - Helper node to delegate the ping to.
     * @param targetId - Suspect node to check.
     */
    private sendPingReqAndWait(helperId: string, targetId: string): Promise<boolean> {
        return new Promise((resolve) => {
            const key = `${helperId}:${targetId}`;
            const timeoutId = setTimeout(() => {
                this.pendingIndirectPongs.delete(key);
                resolve(false);
            }, this.pingTimeout);

            this.pendingIndirectPongs.set(key, () => {
                clearTimeout(timeoutId);
                this.pendingIndirectPongs.delete(key);
                resolve(true);
            });

            this.sendUdpMessage(helperId, {
                type: "PING-REQ",
                from: this.selfNodeId,
                target: targetId,
                members: this.getMembershipView()
            });
        });
    }

    /**
     * Moves a node state to SUSPECT if it was previously ALIVE, setting suspectSince time.
     *
     * @param nodeId - The node transitioning to SUSPECT.
     */
    private markSuspect(nodeId: string): void {
        const info = this.members.get(nodeId);
        if (info && info.state === "ALIVE") {
            info.state = "SUSPECT";
            info.suspectSince = Date.now();
        }
    }

    /**
     * Parses and acts on incoming UDP message payloads.
     * Implements message processing, recovery signals, membership merge rule, and command answering.
     *
     * @param rawMsg - Serialized JSON buffer.
     * @param rinfo - Remote sender information.
     */
    private handleMessage(rawMsg: Buffer, rinfo: dgram.RemoteInfo): void {
        const msgStr = rawMsg.toString("utf8");
        const msg: GossipMessage = JSON.parse(msgStr);

        const senderId = msg.from;
        if (!senderId) return;

        // Ensure sender exists in our known members list
        const senderInfo = this.members.get(senderId);
        if (senderInfo) {
            senderInfo.lastSeen = Date.now();

            if (senderInfo.state === "DEAD") {
                senderInfo.state = "ALIVE";
                senderInfo.suspectSince = undefined;
                this.onNodeAlive(senderId);
            } else if (senderInfo.state === "SUSPECT") {
                senderInfo.state = "ALIVE";
                senderInfo.suspectSince = undefined;
            }
        }

        // 1. Resolve pending direct / indirect responses based on sender
        if (msg.type === "PONG") {
            const resolve = this.pendingPongs.get(senderId);
            if (resolve) resolve();
        } else if (msg.type === "PONG-INDIRECT" && msg.target) {
            const key = `${senderId}:${msg.target}`;
            const resolve = this.pendingIndirectPongs.get(key);
            if (resolve) resolve();

            // Transition the target back to ALIVE if confirmed
            const targetInfo = this.members.get(msg.target);
            if (targetInfo) {
                targetInfo.lastSeen = Date.now();
                if (targetInfo.state === "SUSPECT") {
                    targetInfo.state = "ALIVE";
                    targetInfo.suspectSince = undefined;
                }
            }
        }

        // 2. Merge incoming membership view
        if (msg.members) {
            for (const [peerId, incomingState] of Object.entries(msg.members)) {
                if (peerId === this.selfNodeId) continue;
                const localMember = this.members.get(peerId);
                if (!localMember) continue;

                const localState = localMember.state;
                if (localState === "DEAD") {
                    // Ignore incoming ALIVE or SUSPECT for locally DEAD nodes
                    continue;
                }

                const severityMap = { ALIVE: 1, SUSPECT: 2, DEAD: 3 };
                const incomingSev = severityMap[incomingState as MemberState] ?? 0;
                const localSev = severityMap[localState] ?? 0;

                if (incomingSev > localSev) {
                    localMember.state = incomingState as MemberState;
                    if (incomingState === "SUSPECT") {
                        localMember.suspectSince = Date.now();
                    } else if (incomingState === "DEAD") {
                        localMember.suspectSince = undefined;
                        this.onNodeDead(peerId);
                    }
                }
            }
        }

        // 3. Process commands
        if (msg.type === "PING") {
            this.sendUdpMessage(senderId, {
                type: "PONG",
                from: this.selfNodeId,
                members: this.getMembershipView()
            });
        } else if (msg.type === "PING-REQ" && msg.target) {
            const target = msg.target;
            void this.sendPingAndWait(target).then((success) => {
                if (success) {
                    this.sendUdpMessage(senderId, {
                        type: "PONG-INDIRECT",
                        from: this.selfNodeId,
                        target,
                        members: this.getMembershipView()
                    });
                }
            });
        }
    }

    /**
     * Resolves the UDP address (host, port) for a given nodeId using process.env mappings or default fallback.
     */
    private getNodeAddress(nodeId: string): { host: string; port: number } | undefined {
        const info = this.nodes.find((n) => n.nodeId === nodeId);
        if (!info) return undefined;
        const envKey = `PEER_${info.nodeId.toUpperCase().replace(/-/g, "_")}_HOST`;
        const host = process.env[envKey] ?? (info.host !== "0.0.0.0" ? info.host : "127.0.0.1");
        return { host, port: info.port + 1000 };
    }

    /**
     * Sends a raw serialized GossipMessage to the specified node over UDP.
     */
    private sendUdpMessage(targetId: string, msg: GossipMessage): void {
        if (!this.udpSocket) return;
        const addr = this.getNodeAddress(targetId);
        if (!addr) return;

        const data = Buffer.from(JSON.stringify(msg));
        this.udpSocket.send(data, 0, data.length, addr.port, addr.host, (err) => {
            if (err) {
                console.warn(`[Gossip] Failed to send message to ${targetId}:`, err.message);
            }
        });
    }

    /**
     * Constructs a serializable representation of membership states for infection-style propagation.
     */
    private getMembershipView(): Record<string, MemberState> {
        const view: Record<string, MemberState> = {};
        for (const [nodeId, info] of this.members.entries()) {
            view[nodeId] = info.state;
        }
        view[this.selfNodeId] = "ALIVE";
        return view;
    }
}
