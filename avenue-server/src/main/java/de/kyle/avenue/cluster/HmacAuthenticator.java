package de.kyle.avenue.cluster;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Stateless HMAC-SHA256 helper for the cluster node mutual-authentication handshake.
 * <p>
 * The cluster {@code cluster.secret} is used purely as an HMAC key and is <em>never</em> placed on
 * the wire. Each side proves possession of the shared secret by computing an HMAC over a canonical
 * transcript of the handshake (both node IDs and both nonces) and exchanging only the resulting
 * proof. An attacker observing the wire sees only random nonces and proofs and cannot recover the
 * secret nor forge a valid proof without it.
 * <p>
 * The transcript is domain-separated and role-tagged:
 * <pre>
 *   proof = HMAC-SHA256(secret,
 *       "AVENUE-CLUSTER-AUTH-v1" | nodeId_I | nodeId_A | nonce_I | nonce_A | ROLE)
 * </pre>
 * where {@code ROLE} is {@code "INITIATOR"} or {@code "ACCEPTOR"}. Including the role prevents a
 * reflection attack in which a man-in-the-middle replays the acceptor's proof back as the
 * initiator's proof (or vice versa): the two sides compute HMACs over distinct transcripts, so a
 * proof valid for one role can never satisfy the other. The field separator (a US-ASCII unit
 * separator, {@code 0x1F}) makes the concatenation unambiguous and is independent of any JSON
 * key ordering.
 */
final class HmacAuthenticator {

    /** Protocol/version tag prepended to every transcript for domain separation. */
    static final String PROTOCOL = "AVENUE-CLUSTER-AUTH-v1";

    /** Role tag for the side that opened the TCP connection and sends the first hello. */
    static final String ROLE_INITIATOR = "INITIATOR";

    /** Role tag for the side that accepted the TCP connection and issues the challenge. */
    static final String ROLE_ACCEPTOR = "ACCEPTOR";

    private static final String HMAC_ALGORITHM = "HmacSHA256";

    /** Length of generated nonces in bytes (128 bit of entropy). */
    private static final int NONCE_BYTES = 16;

    /**
     * Canonical field separator. A US-ASCII unit separator ({@code 0x1F}) cannot occur inside a
     * Base64 nonce nor inside a node ID under any realistic configuration, so the concatenation is
     * unambiguous without length-prefixing.
     */
    private static final char FIELD_SEPARATOR = '';

    private final byte[] secretKey;
    private final SecureRandom secureRandom = new SecureRandom();

    /**
     * Creates an authenticator keyed by the shared cluster secret.
     *
     * @param secret the {@code cluster.secret}; used as the HMAC key (UTF-8 encoded)
     */
    HmacAuthenticator(String secret) {
        this.secretKey = (secret != null ? secret : "").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generates a fresh, Base64-encoded nonce with at least 128 bits of entropy.
     *
     * @return a Base64 (standard alphabet) nonce string
     */
    String newNonce() {
        byte[] raw = new byte[NONCE_BYTES];
        secureRandom.nextBytes(raw);
        return Base64.getEncoder().encodeToString(raw);
    }

    /**
     * Computes the role-tagged proof over the shared handshake transcript. Both sides feed in the
     * same {@code nodeId_I}, {@code nodeId_A}, {@code nonce_I} and {@code nonce_A}; only the
     * {@code role} differs, which yields the two distinct proofs exchanged during the handshake.
     * Exposed package-private so it can be unit-tested in isolation.
     *
     * @param initiatorNodeId the initiator's advertised node ID
     * @param acceptorNodeId  the acceptor's advertised node ID
     * @param initiatorNonce  the initiator's Base64 nonce
     * @param acceptorNonce   the acceptor's Base64 nonce
     * @param role            {@link #ROLE_INITIATOR} or {@link #ROLE_ACCEPTOR}
     * @return the Base64-encoded HMAC-SHA256 proof
     */
    String computeProof(
            String initiatorNodeId,
            String acceptorNodeId,
            String initiatorNonce,
            String acceptorNonce,
            String role
    ) {
        String transcript = PROTOCOL
                + FIELD_SEPARATOR + initiatorNodeId
                + FIELD_SEPARATOR + acceptorNodeId
                + FIELD_SEPARATOR + initiatorNonce
                + FIELD_SEPARATOR + acceptorNonce
                + FIELD_SEPARATOR + role;
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secretKey, HMAC_ALGORITHM));
            byte[] hmac = mac.doFinal(transcript.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hmac);
        } catch (GeneralSecurityException e) {
            // HmacSHA256 is mandated by every JVM; a failure here is a fatal environment defect.
            throw new IllegalStateException("HMAC-SHA256 is unavailable in this JVM", e);
        }
    }

    /**
     * Constant-time comparison of two Base64 proofs. Uses {@link MessageDigest#isEqual} so a
     * timing side channel cannot reveal how many leading bytes of a forged proof were correct.
     *
     * @param expected the locally computed proof
     * @param actual   the proof received from the peer
     * @return {@code true} if the proofs are byte-for-byte equal
     */
    boolean proofsMatch(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                actual.getBytes(StandardCharsets.UTF_8));
    }
}
