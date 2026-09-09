package com.anomaly.alertservice.service;

import com.anomaly.alertservice.dto.ScoredWindowMessage;
import com.anomaly.alertservice.entity.MachineEntity;
import com.anomaly.alertservice.repository.MachineRepository;
import com.anomaly.alertservice.repository.TelemetryWindowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Persists scored windows so the dashboard has a real curve to draw (D-41).
 *
 * <p>Same shape as {@link AlertIngestionService} and for the same reasons: an
 * idempotent upsert, because {@code telemetry.scored} is published in update
 * mode and one window arrives repeatedly; and an unknown machine is provisioned
 * rather than rejected, because dropping real measurements over a stale seed is
 * the wrong failure mode (D-39).
 *
 * <p>Windows are stored whether or not they were scored. An unscored window --
 * too few samples, machine stopped, window not yet mature -- still carries real
 * sensor means, and hiding it would leave an unexplained hole in the curve. The
 * row records {@code is_scored} and {@code sample_count} so the dashboard can
 * show the difference instead of pretending there is none.
 */
@Service
public class TelemetryIngestionService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestionService.class);

    private final TelemetryWindowRepository windows;
    private final MachineRepository machines;

    public TelemetryIngestionService(
            TelemetryWindowRepository windows, MachineRepository machines) {
        this.windows = windows;
        this.machines = machines;
    }

    @Transactional
    public void ingest(ScoredWindowMessage message) {
        MachineEntity machine = resolveMachine(message);

        windows.upsert(
                machine.getId(),
                message.windowStart(),
                message.windowEnd(),
                message.sampleCount(),
                message.machineState(),
                Boolean.TRUE.equals(message.scored()),
                message.anomalyScore(),
                message.scoreThreshold(),
                message.anomaly(),
                message.feature(ScoredWindowMessage.TEMPERATURE),
                message.feature(ScoredWindowMessage.VIBRATION),
                message.feature(ScoredWindowMessage.PRESSURE),
                message.feature(ScoredWindowMessage.POWER),
                message.feature(ScoredWindowMessage.ROTATION),
                message.feature(ScoredWindowMessage.NULL_RATIO));
    }

    private MachineEntity resolveMachine(ScoredWindowMessage message) {
        return machines.findByCode(message.machineId())
                .orElseGet(
                        () -> {
                            machines.insertIfAbsent(message.machineId(), message.lineId());
                            log.info(
                                    "machine_auto_provisioned machine_id={} line_id={} source=telemetry",
                                    message.machineId(),
                                    message.lineId());
                            return machines.findByCode(message.machineId())
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "machine "
                                                                    + message.machineId()
                                                                    + " absent immediately after insert"));
                        });
    }
}
