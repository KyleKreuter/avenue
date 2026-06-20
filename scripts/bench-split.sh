#!/usr/bin/env bash
#
# bench-split.sh — out-of-process benchmark runner.
#
# Starts the BenchServer in its OWN java process (separate JVM/scheduler), waits for its
# "BENCH SERVER READY" line, then runs the LoadHarness in a SECOND java process pointed at the
# server via connect=HOST:PORT. Finally it kills the server process. This isolates the server's
# capacity from the load generator (in-JVM runs share CPU/GC/JIT and saturate ~300k msg/s while
# measuring load+server together).
#
# Why plain `java` and not `mvn exec:java`: exec:java runs the main class INSIDE the Maven JVM, so
# wrapping the server that way would not give it a clean, separate process. We build the classpath
# once via Maven and then launch raw `java` processes.
#
# Usage (from repo root):
#   ./scripts/bench-split.sh [PORT] [HARNESS ARGS...]
#
# Examples:
#   # 1:1 saturation headline
#   ./scripts/bench-split.sh 4180 publishers=1 subscribers=1 topics=1 msgSize=100 warmupSeconds=4 seconds=8
#
#   # 1:1 latency at a fixed 120k msg/s working point
#   ./scripts/bench-split.sh 4180 publishers=1 subscribers=1 msgSize=100 rate=120000
#
# The first positional arg is the server PORT (default 4180); everything after it is passed verbatim
# to LoadHarness. The script injects connect=127.0.0.1:PORT automatically, so do NOT pass connect=
# yourself.
#
# Requires JAVA_HOME to point at a JDK 21 (Corretto on the reference machine):
#   export JAVA_HOME=/path/to/corretto-21.0.5/Contents/Home

set -euo pipefail

# --- locate repo root (this script lives in <root>/scripts) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

MVN="${MVN:-/opt/homebrew/bin/mvn}"
JAVA_BIN="${JAVA_HOME:+${JAVA_HOME}/bin/}java"

PORT="${1:-4180}"
shift || true
HARNESS_ARGS=("$@")

MODULE_DIR="${ROOT_DIR}/avenue-server"
CLASSES_DIR="${MODULE_DIR}/target/classes"
TEST_CLASSES_DIR="${MODULE_DIR}/target/test-classes"

echo "==> Compiling test sources (test-compile)"
"${MVN}" -q -pl avenue-server test-compile

echo "==> Building dependency classpath"
CP_FILE="$(mktemp)"
trap 'rm -f "${CP_FILE}"' EXIT
"${MVN}" -q -pl avenue-server dependency:build-classpath \
    -Dmdep.outputFile="${CP_FILE}" >/dev/null
DEP_CP="$(cat "${CP_FILE}")"

# Server needs test-classes (BenchServer) + main classes + all runtime deps.
FULL_CP="${TEST_CLASSES_DIR}:${CLASSES_DIR}:${DEP_CP}"

# --- start the server process in the background ---
SERVER_LOG="$(mktemp)"
trap 'rm -f "${CP_FILE}" "${SERVER_LOG}"' EXIT

echo "==> Starting BenchServer (separate JVM) on port ${PORT}"
"${JAVA_BIN}" -cp "${FULL_CP}" de.kyle.avenue.benchmark.BenchServer "port=${PORT}" \
    >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

# Make sure we always kill the server, even on error/Ctrl-C.
cleanup_server() {
    if kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "==> Stopping BenchServer (pid ${SERVER_PID})"
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
}
trap 'cleanup_server; rm -f "${CP_FILE}" "${SERVER_LOG}"' EXIT INT TERM

# --- wait for the READY line (no fixed sleep) ---
echo "==> Waiting for 'BENCH SERVER READY' ..."
for _ in $(seq 1 300); do
    if grep -q "BENCH SERVER READY" "${SERVER_LOG}" 2>/dev/null; then
        break
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "!! BenchServer exited before becoming ready. Log:"
        cat "${SERVER_LOG}"
        exit 1
    fi
    sleep 0.2
done
if ! grep -q "BENCH SERVER READY" "${SERVER_LOG}" 2>/dev/null; then
    echo "!! Timed out waiting for BenchServer readiness. Log:"
    cat "${SERVER_LOG}"
    exit 1
fi
grep "BENCH SERVER READY" "${SERVER_LOG}"

# --- run the load harness in a SECOND java process ---
echo "==> Running LoadHarness (separate JVM) -> connect=127.0.0.1:${PORT}"
"${JAVA_BIN}" -cp "${FULL_CP}" de.kyle.avenue.benchmark.LoadHarness \
    "connect=127.0.0.1:${PORT}" "${HARNESS_ARGS[@]}"

echo "==> Done."
# Server is torn down by the EXIT trap.
