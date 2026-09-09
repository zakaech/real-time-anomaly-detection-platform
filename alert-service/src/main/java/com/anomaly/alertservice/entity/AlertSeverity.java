package com.anomaly.alertservice.entity;

/**
 * Severity as computed by the stream-processor from how far the score exceeds
 * the threshold.
 *
 * <p>Never recomputed here. It is a model output, and a second implementation
 * downstream would drift from the first without anything failing.
 */
public enum AlertSeverity {
    MEDIUM,
    HIGH,
    CRITICAL
}
