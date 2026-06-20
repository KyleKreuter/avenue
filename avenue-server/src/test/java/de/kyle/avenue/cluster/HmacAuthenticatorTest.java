package de.kyle.avenue.cluster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Base64;

/**
 * Unit tests for {@link HmacAuthenticator}, the HMAC-SHA256 primitive behind the cluster handshake.
 * <p>
 * These tests pin the security-relevant properties independent of the wire protocol:
 * determinism of the proof, sensitivity to every transcript field, role separation
 * (reflection resistance), nonce randomness/length, and constant-time comparison semantics.
 */
class HmacAuthenticatorTest {

    private static final String SECRET = "shared-cluster-secret";

    @Test
    void proof_is_deterministic_for_same_transcript() {
        HmacAuthenticator auth = new HmacAuthenticator(SECRET);
        String a = auth.computeProof("init", "acc", "nonceI", "nonceA", HmacAuthenticator.ROLE_INITIATOR);
        String b = auth.computeProof("init", "acc", "nonceI", "nonceA", HmacAuthenticator.ROLE_INITIATOR);
        Assertions.assertEquals(a, b, "Same transcript must yield the same proof");
    }

    @Test
    void identical_secret_on_both_authenticators_yields_matching_proof() {
        HmacAuthenticator left = new HmacAuthenticator(SECRET);
        HmacAuthenticator right = new HmacAuthenticator(SECRET);
        String l = left.computeProof("i", "a", "nI", "nA", HmacAuthenticator.ROLE_ACCEPTOR);
        String r = right.computeProof("i", "a", "nI", "nA", HmacAuthenticator.ROLE_ACCEPTOR);
        Assertions.assertTrue(left.proofsMatch(l, r), "Equal secrets must produce matching proofs");
    }

    @Test
    void different_secret_yields_non_matching_proof() {
        HmacAuthenticator good = new HmacAuthenticator(SECRET);
        HmacAuthenticator bad = new HmacAuthenticator("totally-different");
        String expected = good.computeProof("i", "a", "nI", "nA", HmacAuthenticator.ROLE_ACCEPTOR);
        String forged = bad.computeProof("i", "a", "nI", "nA", HmacAuthenticator.ROLE_ACCEPTOR);
        Assertions.assertFalse(good.proofsMatch(expected, forged),
                "A different secret must not produce a verifiable proof");
    }

    @Test
    void role_tag_prevents_reflection() {
        // Same transcript, only the role differs -> proofs must differ, so an acceptor proof can
        // never be replayed as a valid initiator proof.
        HmacAuthenticator auth = new HmacAuthenticator(SECRET);
        String asInitiator = auth.computeProof("i", "a", "nI", "nA", HmacAuthenticator.ROLE_INITIATOR);
        String asAcceptor = auth.computeProof("i", "a", "nI", "nA", HmacAuthenticator.ROLE_ACCEPTOR);
        Assertions.assertNotEquals(asInitiator, asAcceptor,
                "Initiator and acceptor proofs over the same nonces must differ");
    }

    @Test
    void every_transcript_field_changes_the_proof() {
        HmacAuthenticator auth = new HmacAuthenticator(SECRET);
        String base = auth.computeProof("i", "a", "nI", "nA", HmacAuthenticator.ROLE_INITIATOR);
        Assertions.assertNotEquals(base,
                auth.computeProof("X", "a", "nI", "nA", HmacAuthenticator.ROLE_INITIATOR));
        Assertions.assertNotEquals(base,
                auth.computeProof("i", "X", "nI", "nA", HmacAuthenticator.ROLE_INITIATOR));
        Assertions.assertNotEquals(base,
                auth.computeProof("i", "a", "X", "nA", HmacAuthenticator.ROLE_INITIATOR));
        Assertions.assertNotEquals(base,
                auth.computeProof("i", "a", "nI", "X", HmacAuthenticator.ROLE_INITIATOR));
    }

    @Test
    void nonce_is_random_and_has_at_least_128_bits() {
        HmacAuthenticator auth = new HmacAuthenticator(SECRET);
        String n1 = auth.newNonce();
        String n2 = auth.newNonce();
        Assertions.assertNotEquals(n1, n2, "Consecutive nonces must differ");
        byte[] decoded = Base64.getDecoder().decode(n1);
        Assertions.assertTrue(decoded.length >= 16, "Nonce must carry at least 128 bits of entropy");
    }

    @Test
    void proofs_match_is_null_safe() {
        HmacAuthenticator auth = new HmacAuthenticator(SECRET);
        Assertions.assertFalse(auth.proofsMatch(null, "x"));
        Assertions.assertFalse(auth.proofsMatch("x", null));
        Assertions.assertFalse(auth.proofsMatch(null, null));
    }
}
