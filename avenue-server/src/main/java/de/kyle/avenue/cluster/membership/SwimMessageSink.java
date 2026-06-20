package de.kyle.avenue.cluster.membership;

import java.util.List;

/**
 * Receiving-side entry point for SWIM control traffic (Phase E). A {@link de.kyle.avenue.cluster.PeerLink}
 * parses an incoming SWIM packet and routes it to the node's {@link SwimMembership} through this sink,
 * passing the authenticated sender's node id so the membership layer can correlate probes and gossip
 * with the link they arrived on.
 */
public interface SwimMessageSink {

    void onSwimPing(String fromNodeId, long seqNo, List<GossipUpdate> piggyback);

    void onSwimAck(String fromNodeId, long seqNo, List<GossipUpdate> piggyback);

    void onSwimPingReq(String fromNodeId, String targetNodeId, long seqNo, List<GossipUpdate> piggyback);

    void onSwimPingReqAck(String fromNodeId, String targetNodeId, long seqNo, List<GossipUpdate> piggyback);

    void onSwimJoin(String fromNodeId, String host, int clusterPort, long incarnation);

    void onSwimJoinAck(String fromNodeId, List<GossipUpdate> memberList);

    void onSwimLeave(String fromNodeId, long incarnation);
}
