package de.kyle.avenue.integration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Generates a deterministic, self-signed PKCS12 keystore + matching truststore at runtime so
 * the TLS integration test needs no checked-in certificate files.
 *
 * <p>The keystore is produced with {@code keytool} (always present in a JDK) via a
 * {@link ProcessBuilder}. {@code keytool} is invoked from {@code $JAVA_HOME/bin} (falling back
 * to the PATH) so the test is self-contained. The certificate uses {@code CN=localhost} and a
 * {@code SAN=DNS:localhost,IP:127.0.0.1} so a TLS client connecting to {@code 127.0.0.1} can
 * verify it. The same store is reused as a truststore: since the cert is self-signed, trusting
 * the keystore that holds it is sufficient for the cluster client side.
 */
final class TlsTestKeystore {

    static final String PASSWORD = "changeit";
    static final String ALIAS = "avenue-test";

    private TlsTestKeystore() {
    }

    /**
     * Creates a self-signed PKCS12 keystore at the given path. Idempotent: if the file already
     * exists it is left untouched.
     *
     * @return the absolute path to the generated keystore
     */
    static Path generate(Path keystorePath) throws IOException, InterruptedException {
        if (Files.exists(keystorePath)) {
            return keystorePath.toAbsolutePath();
        }
        Files.createDirectories(keystorePath.getParent());

        String keytool = resolveKeytool();
        List<String> command = List.of(
                keytool,
                "-genkeypair",
                "-alias", ALIAS,
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "365",
                "-dname", "CN=localhost,OU=Avenue,O=Avenue,L=Test,ST=Test,C=DE",
                "-ext", "SAN=DNS:localhost,IP:127.0.0.1",
                "-storetype", "PKCS12",
                "-keystore", keystorePath.toAbsolutePath().toString(),
                "-storepass", PASSWORD,
                "-keypass", PASSWORD
        );

        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        boolean finished = process.waitFor(60, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IOException("keytool did not finish within timeout");
        }
        if (process.exitValue() != 0 || !Files.exists(keystorePath)) {
            throw new IOException("keytool failed (exit=" + process.exitValue() + "):\n" + output);
        }
        return keystorePath.toAbsolutePath();
    }

    private static String resolveKeytool() {
        String javaHome = System.getProperty("java.home");
        if (javaHome != null && !javaHome.isBlank()) {
            Path candidate = Path.of(javaHome, "bin", "keytool");
            if (Files.isExecutable(candidate)) {
                return candidate.toString();
            }
        }
        // Fall back to PATH lookup.
        return "keytool";
    }
}
