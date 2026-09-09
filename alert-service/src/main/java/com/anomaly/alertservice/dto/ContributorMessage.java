package com.anomaly.alertservice.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * One feature that departed most from the training reference.
 *
 * <p>This is what turns a score into something an operator can act on, and it is
 * the only per-feature detail an alert carries: the full 52-feature vector is
 * never parsed by the alerting query, so it is not available downstream.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ContributorMessage(
        @NotBlank String feature, @JsonProperty("z_score") @NotNull Double zScore) {}
