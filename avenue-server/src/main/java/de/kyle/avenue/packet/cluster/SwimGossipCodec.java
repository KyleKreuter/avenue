package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.cluster.membership.GossipUpdate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Tiny shared (de)serialization helper for the {@code piggyback} / {@code memberList} arrays of
 * {@link GossipUpdate}s carried by the SWIM packets (Phase E). Keeps each packet class free of the
 * repeated array marshalling.
 */
final class SwimGossipCodec {

    private SwimGossipCodec() {
    }

    static JSONArray toJsonArray(List<GossipUpdate> updates) {
        JSONArray arr = new JSONArray();
        if (updates != null) {
            for (GossipUpdate u : updates) {
                arr.put(u.toJson());
            }
        }
        return arr;
    }

    static List<GossipUpdate> fromJsonArray(JSONArray arr) {
        List<GossipUpdate> result = new ArrayList<>();
        if (arr != null) {
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.optJSONObject(i);
                if (o != null) {
                    result.add(GossipUpdate.fromJson(o));
                }
            }
        }
        return result;
    }
}
