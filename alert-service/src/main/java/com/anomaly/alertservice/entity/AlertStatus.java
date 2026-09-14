package com.anomaly.alertservice.entity;

/**
 * Lifecycle of an alert inside this service.
 *
 * <p><strong>This is not the state of the Spark detector.</strong> The
 * stream-processor keeps its own notion of an alert episode being open or closed
 * (closed after 120 s of silence), and that state never leaves the job: it
 * emits no {@code CLOSED} alert. What arrives on the wire is always
 * {@code NEW}; everything below belongs to the operator, and the two must never
 * be reconciled.
 *
 * <p>All four values are carried by the database CHECK constraint even though
 * only NEW to ACKNOWLEDGED is implemented, so adding the rest later needs no
 * migration.
 */
public enum AlertStatus {
    NEW,
    ACKNOWLEDGED,
    RESOLVED,
    DISMISSED
}
