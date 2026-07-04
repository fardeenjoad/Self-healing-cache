import { NodeInfo, ClusterConfig } from "../types/index.js";

export const CLUSTER_CONFIG: ClusterConfig = [
    { nodeId: "node-a", host: "0.0.0.0", port: 7001 },
    { nodeId: "node-b", host: "0.0.0.0", port: 7002 },
    { nodeId: "node-c", host: "0.0.0.0", port: 7003 },
] as const;

export function getNodeInfo(nodeId: string): NodeInfo | undefined {
    return CLUSTER_CONFIG.find((node) => node.nodeId === nodeId);
}
