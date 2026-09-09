package com.anomaly.alertservice.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;

/**
 * Which model produced the score.
 *
 * <p>trainedAt and artifactSha256 are optional in the contract and were null on
 * every alert until the Phase 3 projection was fixed to carry them. The sha256
 * is what ties an alert to the exact binary that scored it.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ModelRefMessage(
        @NotBlank String name,
        @NotBlank @Pattern(regexp = "^[0-9]+\\.[0-9]+\\.[0-9]+$") String version,
        @JsonProperty("trained_at") Instant trainedAt,
        @JsonProperty("artifact_sha256") @Pattern(regexp = "^[0-9a-f]{64}$") String artifactSha256) {}
