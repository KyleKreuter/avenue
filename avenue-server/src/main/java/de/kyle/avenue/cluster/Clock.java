package de.kyle.avenue.cluster;

/**
 * Minimal monotonic clock seam used by {@link PeerLink} for the heartbeat watchdog time
 * comparisons. Injecting a clock lets tests drive the watchdog deterministically (e.g. to
 * simulate a heartbeat timeout) without real wall-clock sleeps.
 * <p>
 * The production default is {@link #SYSTEM}, backed by {@link System#nanoTime()}.
 */
@FunctionalInterface
public interface Clock {

    /** System clock backed by {@link System#nanoTime()}. */
    Clock SYSTEM = System::nanoTime;

    /**
     * Returns the current value of the most precise available system timer, in nanoseconds.
     * Only differences between two reads are meaningful (same contract as
     * {@link System#nanoTime()}).
     */
    long nanoTime();
}
