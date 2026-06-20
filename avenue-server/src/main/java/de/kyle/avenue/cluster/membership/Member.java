package de.kyle.avenue.cluster.membership;

/**
 * A single remote cluster member tracked by the {@link MemberRegistry} (Phase E SWIM membership).
 * <p>
 * The local node is never represented here — the registry only tracks <em>remote</em> peers. The
 * mutable fields ({@link #incarnation}, {@link #state}, {@link #stateChangedAtMillis}) are
 * {@code volatile} and only mutated under the registry's per-member synchronization, so reads on the
 * gossip / probe paths see consistent values without locking.
 */
public class Member {

    private final String nodeId;
    private final String host;
    private final int clusterPort;

    private volatile long incarnation;
    private volatile MemberState state;
    private volatile long stateChangedAtMillis;

    public Member(String nodeId, String host, int clusterPort, long incarnation,
                  MemberState state, long stateChangedAtMillis) {
        this.nodeId = nodeId;
        this.host = host;
        this.clusterPort = clusterPort;
        this.incarnation = incarnation;
        this.state = state;
        this.stateChangedAtMillis = stateChangedAtMillis;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getClusterPort() {
        return clusterPort;
    }

    public long getIncarnation() {
        return incarnation;
    }

    void setIncarnation(long incarnation) {
        this.incarnation = incarnation;
    }

    public MemberState getState() {
        return state;
    }

    void setState(MemberState state) {
        this.state = state;
    }

    public long getStateChangedAtMillis() {
        return stateChangedAtMillis;
    }

    void setStateChangedAtMillis(long stateChangedAtMillis) {
        this.stateChangedAtMillis = stateChangedAtMillis;
    }

    @Override
    public String toString() {
        return "Member{" + nodeId + ", " + host + ":" + clusterPort
                + ", inc=" + incarnation + ", state=" + state + '}';
    }
}
