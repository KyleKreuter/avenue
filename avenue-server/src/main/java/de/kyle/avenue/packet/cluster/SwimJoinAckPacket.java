package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.cluster.membership.GossipUpdate;
import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * SWIM join acknowledgement (Phase E). Sent by a seed in reply to a {@link SwimJoinPacket}: a full
 * snapshot of every member the seed knows (including the joining node's own last-known incarnation,
 * if any, so the joiner can bump past a stale record and the seed itself). Merging this list bootstraps
 * the newcomer's view and triggers outbound connections to the discovered members.
 */
public class SwimJoinAckPacket implements Packet {

    private final List<GossipUpdate> memberList;

    public SwimJoinAckPacket(List<GossipUpdate> memberList) {
        this.memberList = memberList != null ? memberList : List.of();
    }

    public List<GossipUpdate> getMemberList() {
        return memberList;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject obj = new JSONObject();
        obj.put("memberList", SwimGossipCodec.toJsonArray(memberList));
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static SwimJoinAckPacket fromJson(JSONObject envelope) {
        JSONObject body = envelope.getJSONObject("body");
        return new SwimJoinAckPacket(SwimGossipCodec.fromJsonArray(body.optJSONArray("memberList")));
    }
}
