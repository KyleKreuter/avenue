package de.kyle.avenue.message;

/**
 * A delivered pub/sub message handed to a {@code TopicListener}.
 * <p>
 * Public API decision (perf stage 1.5): the payload on the wire is now an opaque protobuf
 * {@code bytes} field so the server can pass it through without per-message UTF-8 transcoding.
 * To keep the existing public client API source- and behaviour-compatible, {@code data()} stays a
 * {@code String}: {@link de.kyle.avenue.AvenueClient} decodes the payload's UTF-8 bytes to a String
 * exactly once at the client API edge before constructing this record. That single decode is
 * client-side only and never touches the server hot path.
 *
 * @param source the publisher's logical source name
 * @param data   the UTF-8-decoded message payload
 */
public record Message(String source, String data) {
}
