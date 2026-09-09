package com.anomaly.alertservice.exception;

/**
 * A message that is wrong in itself: malformed JSON, a schema violation, an
 * unsupported major version.
 *
 * <p>This is the boundary between the two error classes. Anything thrown as this
 * goes straight to the dead letter topic with no retry, because replaying an
 * invalid message will never make it valid. Everything else -- a database
 * outage, a timeout -- is transient and must be retried indefinitely rather than
 * discarded: routing infrastructure failures to a dead letter queue would throw
 * away perfectly good alerts exactly when losing them costs most (ADR-002).
 */
public class NonRetryableMessageException extends RuntimeException {

    private final String reason;

    public NonRetryableMessageException(String reason, String detail) {
        super(detail);
        this.reason = reason;
    }

    public NonRetryableMessageException(String reason, String detail, Throwable cause) {
        super(detail, cause);
        this.reason = reason;
    }

    /** Maps to the dlq_reason of the DlqEnvelope contract. */
    public String getReason() {
        return reason;
    }
}
