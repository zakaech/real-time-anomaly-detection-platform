package com.anomaly.alertservice.mapper;

import com.anomaly.alertservice.dto.AlertDetail;
import com.anomaly.alertservice.dto.AlertStreamEvent;
import com.anomaly.alertservice.dto.AlertSummary;
import com.anomaly.alertservice.dto.ContributorMessage;
import com.anomaly.alertservice.entity.AlertAcknowledgementEntity;
import com.anomaly.alertservice.entity.AlertEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Entity to DTO, in one place.
 *
 * <p>JPA entities never leave the service layer. Serialising them directly would
 * publish the database shape as the API contract, drag lazy associations into
 * the JSON writer, and make any column rename a breaking change for clients.
 */
@Component
public class AlertMapper {

    private static final Logger log = LoggerFactory.getLogger(AlertMapper.class);
    private static final TypeReference<List<ContributorMessage>> CONTRIBUTORS =
            new TypeReference<>() {};

    private final ObjectMapper objectMapper;

    public AlertMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public AlertSummary toSummary(AlertEntity alert) {
        return new AlertSummary(
                alert.getId(),
                alert.getMachine().getCode(),
                alert.getMachine().getLineCode(),
                alert.getSeverity(),
                alert.getStatus(),
                alert.getAnomalyScore(),
                alert.getScoreThreshold(),
                alert.getDetectedAt(),
                alert.getConsecutiveWindows());
    }

    public AlertDetail toDetail(AlertEntity alert, List<AlertAcknowledgementEntity> history) {
        return new AlertDetail(
                alert.getId(),
                alert.getMachine().getCode(),
                alert.getMachine().getLineCode(),
                alert.getMachine().getMachineType(),
                alert.getSeverity(),
                alert.getStatus(),
                alert.getAnomalyScore(),
                alert.getScoreThreshold(),
                alert.getDetectedAt(),
                alert.getWindowStart(),
                alert.getWindowEnd(),
                alert.getPublishedAt(),
                alert.getIngestedAt(),
                alert.getConsecutiveWindows(),
                new AlertDetail.ModelInfo(
                        alert.getModelName(),
                        alert.getModelVersion(),
                        alert.getModelTrainedAt(),
                        alert.getModelArtifactSha256()),
                readContributors(alert),
                history.stream()
                        .map(
                                entry ->
                                        new AlertDetail.AcknowledgementEntry(
                                                entry.getPreviousStatus(),
                                                entry.getNewStatus(),
                                                entry.getActor(),
                                                entry.getComment(),
                                                entry.getOccurredAt()))
                        .toList(),
                alert.getOptlockVersion());
    }

    public AlertStreamEvent toStreamEvent(AlertEntity alert) {
        return new AlertStreamEvent(
                alert.getId(),
                alert.getEventSeq(),
                alert.getMachine().getCode(),
                alert.getMachine().getLineCode(),
                alert.getSeverity(),
                alert.getStatus(),
                alert.getAnomalyScore(),
                alert.getDetectedAt());
    }

    private List<ContributorMessage> readContributors(AlertEntity alert) {
        String raw = alert.getTopContributors();
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        try {
            return objectMapper.readValue(raw, CONTRIBUTORS);
        } catch (JsonProcessingException e) {
            // Stored JSON that will not parse is a data problem worth seeing in
            // the logs, but it must not turn a readable alert into a 500.
            log.warn("alert_contributors_unreadable alert_id={} error={}", alert.getId(), e.getOriginalMessage());
            return List.of();
        }
    }
}
