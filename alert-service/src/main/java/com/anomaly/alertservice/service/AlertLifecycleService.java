package com.anomaly.alertservice.service;

import com.anomaly.alertservice.dto.AcknowledgeRequest;
import com.anomaly.alertservice.dto.AlertDetail;
import com.anomaly.alertservice.entity.AlertAcknowledgementEntity;
import com.anomaly.alertservice.entity.AlertEntity;
import com.anomaly.alertservice.entity.AlertStatus;
import com.anomaly.alertservice.exception.AlertNotFoundException;
import com.anomaly.alertservice.exception.InvalidStateTransitionException;
import com.anomaly.alertservice.mapper.AlertMapper;
import com.anomaly.alertservice.repository.AlertAcknowledgementRepository;
import com.anomaly.alertservice.repository.AlertRepository;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Status transitions driven by an operator.
 *
 * <p>Only one transition is implemented, NEW to ACKNOWLEDGED. The database
 * CHECK already accepts RESOLVED and DISMISSED, so extending this later needs
 * no migration.
 */
@Service
public class AlertLifecycleService {

    private static final Logger log = LoggerFactory.getLogger(AlertLifecycleService.class);

    private final AlertRepository alerts;
    private final AlertAcknowledgementRepository acknowledgements;
    private final AlertMapper mapper;
    private final ApplicationEventPublisher events;

    public AlertLifecycleService(
            AlertRepository alerts,
            AlertAcknowledgementRepository acknowledgements,
            AlertMapper mapper,
            ApplicationEventPublisher events) {
        this.alerts = alerts;
        this.acknowledgements = acknowledgements;
        this.mapper = mapper;
        this.events = events;
    }

    /**
     * Acknowledge an alert.
     *
     * <p>One transaction covers both the status change and the audit row: neither
     * an acknowledged alert with no record of who acknowledged it, nor a record
     * of an acknowledgement that did not happen, may exist.
     *
     * @throws AlertNotFoundException if no such alert exists
     * @throws InvalidStateTransitionException if it is not NEW, or if
     *     {@code expectedVersion} no longer matches
     */
    @Transactional
    public AlertDetail acknowledge(UUID alertId, AcknowledgeRequest request, String actor) {
        AlertEntity alert =
                alerts.findByIdWithMachine(alertId)
                        .orElseThrow(() -> new AlertNotFoundException(alertId));

        if (alert.getStatus() != AlertStatus.NEW) {
            throw new InvalidStateTransitionException(
                    alertId,
                    alert.getStatus(),
                    alert.getOptlockVersion(),
                    "Alert " + alertId + " is " + alert.getStatus() + " and cannot be acknowledged");
        }

        // Optional by design (decision D-09): a dashboard acting on a freshly
        // loaded row should not be forced to carry a version, but an operator
        // who wants the guarantee can have it. Without any check at all, two
        // operators act, one of them silently loses, and neither is told.
        if (request.expectedVersion() != null
                && request.expectedVersion() != alert.getOptlockVersion()) {
            throw new InvalidStateTransitionException(
                    alertId,
                    alert.getStatus(),
                    alert.getOptlockVersion(),
                    "Alert "
                            + alertId
                            + " was modified concurrently: expected version "
                            + request.expectedVersion()
                            + ", current version "
                            + alert.getOptlockVersion());
        }

        Instant now = Instant.now();
        AlertStatus previous = alert.getStatus();
        alert.transitionTo(AlertStatus.ACKNOWLEDGED, now);

        acknowledgements.save(
                new AlertAcknowledgementEntity(
                        alertId,
                        previous,
                        AlertStatus.ACKNOWLEDGED,
                        actor,
                        request.comment(),
                        now));

        log.info(
                "alert_acknowledged alert_id={} machine_id={} actor={} previous_status={}",
                alertId,
                alert.getMachine().getCode(),
                actor,
                previous);

        // Raised here, delivered by the broadcaster AFTER the commit, the same
        // pattern ingestion uses for alert.created. Broadcasting from inside this
        // transaction would let a failing client connection roll the
        // acknowledgement back.
        events.publishEvent(new AlertBroadcaster.AlertUpdatedEvent(alertId));
        return mapper.toDetail(alert, acknowledgements.findByAlertIdOrderByOccurredAtDesc(alertId));
    }
}
