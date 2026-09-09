package com.anomaly.alertservice.dto;

import jakarta.validation.constraints.Size;

/**
 * Body of an acknowledgement.
 *
 * <p>expectedVersion is optional (decision D-09). When present, the update is
 * rejected with 409 if another operator changed the alert first; when absent,
 * no concurrency check is made. Making it mandatory would burden a dashboard
 * that mostly acts on freshly loaded rows; making it impossible would leave the
 * silent failure where two operators act and only one of them counts, with
 * neither being told.
 */
public record AcknowledgeRequest(
        @Size(max = 1000, message = "comment must not exceed 1000 characters") String comment,
        Long expectedVersion) {}
