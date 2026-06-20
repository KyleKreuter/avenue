package de.kyle.avenue.admin.dto;

/**
 * Immutable, read-only view of a single active peer link for admin introspection (Phase F).
 * <p>
 * Built from the live {@link de.kyle.avenue.cluster.PeerLink} plus the per-target send state in
 * {@link de.kyle.avenue.cluster.ClusterManager}. Only derived, copied scalars are exposed — no live
 * queue / buffer reference escapes.
 *
 * @param nodeId           the remote peer's node id
 * @param running          whether the link's reader/writer loops are currently running
 * @param replayDepth      number of sent-but-unacked frames buffered for this target (backfill depth)
 * @param ingestDepth      number of publishes queued for the writer but not yet appended/sent
 * @param outboundLinkSeq  highest per-target reliability sequence assigned to this peer so far
 * @param contiguousLinkSeq highest contiguous reliability sequence received from this peer (ACK frontier)
 */
public record PeerSnapshot(
        String nodeId,
        boolean running,
        int replayDepth,
        int ingestDepth,
        long outboundLinkSeq,
        long contiguousLinkSeq
) {
}
