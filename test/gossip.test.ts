import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { GossipManager } from "../src/node/GossipManager.js";
import type { NodeInfo } from "../src/types/index.js";

class MockSocket {
    listeners = new Map<string, Function[]>();
    isClosed = false;
    boundPort?: number;
    boundHost?: string;
    sentMessages: Array<{ msg: Buffer; port: number; host: string }> = [];

    on(event: string, handler: Function) {
        if (!this.listeners.has(event)) {
            this.listeners.set(event, []);
        }
        this.listeners.get(event)!.push(handler);
        return this;
    }

    bind(port: number, host: string, callback?: Function) {
        this.boundPort = port;
        this.boundHost = host;
        if (callback) callback();
        const list = this.listeners.get("listening");
        if (list) {
            for (const h of list) h();
        }
    }

    send(msg: Buffer, offset: number, length: number, port: number, host: string, callback?: Function) {
        this.sentMessages.push({ msg, port, host });
        if (callback) callback();
    }

    close() {
        this.isClosed = true;
        const list = this.listeners.get("close");
        if (list) {
            for (const h of list) h();
        }
    }

    receiveMessage(msgObj: any, rinfo: { address: string; port: number }) {
        const list = this.listeners.get("message");
        if (list) {
            const buffer = Buffer.from(JSON.stringify(msgObj));
            for (const h of list) h(buffer, rinfo);
        }
    }
}

let activeSocket: MockSocket | null = null;

vi.mock("node:dgram", () => {
    return {
        default: {
            createSocket: vi.fn().mockImplementation(() => {
                activeSocket = new MockSocket();
                return activeSocket;
            })
        }
    };
});
vi.mock("dgram", () => {
    return {
        default: {
            createSocket: vi.fn().mockImplementation(() => {
                activeSocket = new MockSocket();
                return activeSocket;
            })
        }
    };
});

describe("GossipManager", () => {
    const nodes: NodeInfo[] = [
        { nodeId: "node-a", host: "127.0.0.1", port: 7001 },
        { nodeId: "node-b", host: "127.0.0.1", port: 7002 },
        { nodeId: "node-c", host: "127.0.0.1", port: 7003 }
    ];

    beforeEach(() => {
        activeSocket = null;
        vi.useFakeTimers();
        vi.clearAllMocks();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it("Initial state — all peers start as ALIVE", () => {
        const manager = new GossipManager("node-a", nodes, () => {}, () => {});
        expect(manager.getMemberState("node-b")).toBe("ALIVE");
        expect(manager.getMemberState("node-c")).toBe("ALIVE");
        expect(manager.isAlive("node-b")).toBe(true);
        expect(manager.isAlive("node-c")).toBe(true);
    });

    it("PING timeout → SUSPECT", async () => {
        const manager = new GossipManager("node-a", nodes, () => {}, () => {}, 10000, 6000, 2000);
        manager.start();

        // Advance to trigger first gossip round (at 10000ms)
        await vi.advanceTimersByTimeAsync(10000);

        // We expect a PING message to be sent to either node-b or node-c
        expect(activeSocket).not.toBeNull();
        expect(activeSocket!.sentMessages.length).toBe(1);

        const firstSent = JSON.parse(activeSocket!.sentMessages[0].msg.toString());
        expect(firstSent.type).toBe("PING");
        const targetNodeId = activeSocket!.sentMessages[0].port === 8002 ? "node-b" : "node-c";

        // Advance 2000ms (direct timeout) to trigger indirect ping-req
        await vi.advanceTimersByTimeAsync(2000);

        // Expect second sent message to be PING-REQ
        expect(activeSocket!.sentMessages.length).toBe(2);
        const secondSent = JSON.parse(activeSocket!.sentMessages[1].msg.toString());
        expect(secondSent.type).toBe("PING-REQ");
        expect(secondSent.target).toBe(targetNodeId);

        // Advance 2000ms (indirect timeout)
        await vi.advanceTimersByTimeAsync(2000);

        // Target should now be SUSPECT
        expect(manager.getMemberState(targetNodeId)).toBe("SUSPECT");
        manager.stop();
    });

    it("SUSPECT timeout → DEAD", async () => {
        let deadCalled: string | null = null;
        const manager = new GossipManager(
            "node-a",
            nodes,
            (id) => { deadCalled = id; },
            () => {},
            10000,
            6000,
            2000
        );
        manager.start();

        // Advance to trigger first gossip round (at 10000ms)
        await vi.advanceTimersByTimeAsync(10000);
        const targetNodeId = activeSocket!.sentMessages[0].port === 8002 ? "node-b" : "node-c";

        // Wait direct ping timeout (2000ms) + indirect ping timeout (2000ms) to become SUSPECT
        await vi.advanceTimersByTimeAsync(4000);
        expect(manager.getMemberState(targetNodeId)).toBe("SUSPECT");

        // Advance 6000ms to reach next round and check suspect timeout
        await vi.advanceTimersByTimeAsync(6000);
        expect(manager.getMemberState(targetNodeId)).toBe("DEAD");
        expect(deadCalled).toBe(targetNodeId);
        manager.stop();
    });

    it("Recovery", async () => {
        let aliveCalled: string | null = null;
        let deadCalled: string | null = null;
        const manager = new GossipManager(
            "node-a",
            nodes,
            (id) => { deadCalled = id; },
            (id) => { aliveCalled = id; }
        );
        manager.start();

        // Force a peer to DEAD state locally
        const members = manager.getAllMembers();
        const peer = members.get("node-b");
        if (peer) {
            peer.state = "DEAD";
        }
        expect(manager.getMemberState("node-b")).toBe("DEAD");

        // Simulate incoming message from node-b
        activeSocket!.receiveMessage({
            type: "PING",
            from: "node-b"
        }, { address: "127.0.0.1", port: 8002 });

        expect(manager.getMemberState("node-b")).toBe("ALIVE");
        expect(aliveCalled).toBe("node-b");
        manager.stop();
    });

    it("Membership merge", async () => {
        const manager = new GossipManager("node-a", nodes, () => {}, () => {});
        manager.start();

        // Simulate incoming membership saying node-c is SUSPECT
        activeSocket!.receiveMessage({
            type: "PING",
            from: "node-b",
            members: {
                "node-b": "ALIVE",
                "node-c": "SUSPECT"
            }
        }, { address: "127.0.0.1", port: 8002 });

        expect(manager.getMemberState("node-c")).toBe("SUSPECT");
        manager.stop();
    });

    it("Membership merge rule", async () => {
        const manager = new GossipManager("node-a", nodes, () => {}, () => {});
        manager.start();

        // Force node-c to DEAD locally
        const members = manager.getAllMembers();
        const peer = members.get("node-c");
        if (peer) {
            peer.state = "DEAD";
        }

        // Simulate incoming membership from node-b saying node-c is ALIVE
        activeSocket!.receiveMessage({
            type: "PING",
            from: "node-b",
            members: {
                "node-b": "ALIVE",
                "node-c": "ALIVE"
            }
        }, { address: "127.0.0.1", port: 8002 });

        // node-c should remain DEAD (merge exception rule)
        expect(manager.getMemberState("node-c")).toBe("DEAD");
        manager.stop();
    });

    it("PING-REQ flow", async () => {
        const manager = new GossipManager("node-a", nodes, () => {}, () => {}, 10000, 6000, 2000);
        manager.start();

        // Advance to trigger first gossip round
        await vi.advanceTimersByTimeAsync(10000);
        expect(activeSocket!.sentMessages.length).toBe(1);
        const firstSent = JSON.parse(activeSocket!.sentMessages[0].msg.toString());
        const targetNodeId = activeSocket!.sentMessages[0].port === 8002 ? "node-b" : "node-c";
        const helperNodeId = targetNodeId === "node-b" ? "node-c" : "node-b";

        // Advance 2000ms (direct timeout) to trigger PING-REQ
        await vi.advanceTimersByTimeAsync(2000);
        expect(activeSocket!.sentMessages.length).toBe(2);
        
        const secondSent = JSON.parse(activeSocket!.sentMessages[1].msg.toString());
        expect(secondSent.type).toBe("PING-REQ");
        expect(secondSent.target).toBe(targetNodeId);
        const expectedHelperPort = helperNodeId === "node-b" ? 8002 : 8003;
        expect(activeSocket!.sentMessages[1].port).toBe(expectedHelperPort);
        
        manager.stop();
    });

    it("Dead node skip", async () => {
        const manager = new GossipManager("node-a", nodes, () => {}, () => {}, 2000, 6000, 2000);
        manager.start();

        // Mark node-b as DEAD
        const members = manager.getAllMembers();
        const peerB = members.get("node-b");
        if (peerB) peerB.state = "DEAD";

        // Trigger multiple gossip rounds
        for (let i = 0; i < 5; i++) {
            await vi.advanceTimersByTimeAsync(2000);
        }

        // Verify no messages were sent to node-b (port 8002)
        const sentToB = activeSocket!.sentMessages.filter(m => m.port === 8002);
        expect(sentToB.length).toBe(0);
        manager.stop();
    });
});
