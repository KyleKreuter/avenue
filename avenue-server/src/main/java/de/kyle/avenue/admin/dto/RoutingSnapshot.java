package de.kyle.avenue.admin.dto;

import java.util.List;
import java.util.Map;

/**
 * Immutable, read-only view of the interest routing table for admin introspection (Phase F).
 * <p>
 * {@link #topicToNodes} is a deep, unmodifiable copy ({@code topic -> interested remote node ids})
 * of the live forward index, so the admin layer can never mutate or observe the live concurrent
 * sets. {@link #topicCount} mirrors the {@code routingTableTopicCount} gauge.
 *
 * @param topicCount   number of topics that currently have at least one interested remote node
 * @param topicToNodes unmodifiable map of topic to the sorted list of interested remote node ids
 */
public record RoutingSnapshot(
        int topicCount,
        Map<String, List<String>> topicToNodes
) {
}
