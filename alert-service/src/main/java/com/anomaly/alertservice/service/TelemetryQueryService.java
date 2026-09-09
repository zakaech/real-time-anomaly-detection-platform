package com.anomaly.alertservice.service;

import com.anomaly.alertservice.dto.TelemetrySeries;
import com.anomaly.alertservice.entity.MachineEntity;
import com.anomaly.alertservice.entity.TelemetryWindowEntity;
import com.anomaly.alertservice.exception.MachineNotFoundException;
import com.anomaly.alertservice.repository.MachineRepository;
import com.anomaly.alertservice.repository.TelemetryWindowRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Serves the dashboard curve. */
@Service
public class TelemetryQueryService {

    private final TelemetryWindowRepository windows;
    private final MachineRepository machines;

    public TelemetryQueryService(
            TelemetryWindowRepository windows, MachineRepository machines) {
        this.windows = windows;
        this.machines = machines;
    }

    /**
     * The windows of one machine over a range, in chronological order.
     *
     * <p>The result is bounded by {@code maxPoints}. A 10-second window over a
     * month is a quarter of a million rows, and letting a client ask for that
     * would turn a chart endpoint into a way to pull the table through the API.
     * When the limit bites, the response says so.
     */
    @Transactional(readOnly = true)
    public TelemetrySeries series(String machineCode, Instant from, Instant to, int maxPoints) {
        MachineEntity machine =
                machines.findByCode(machineCode)
                        .orElseThrow(() -> new MachineNotFoundException(machineCode));

        // One extra row is requested so the caller can be told the range was cut
        // rather than left to guess from a suspiciously round count.
        List<TelemetryWindowEntity> rows =
                windows.findRange(machine.getId(), from, to, PageRequest.of(0, maxPoints + 1));

        boolean truncated = rows.size() > maxPoints;
        if (truncated) {
            rows = rows.subList(0, maxPoints);
        }

        return new TelemetrySeries(
                machine.getCode(),
                machine.getLineCode(),
                from,
                to,
                windowSeconds(rows),
                truncated,
                rows.stream()
                        .map(
                                row ->
                                        new TelemetrySeries.TelemetryPoint(
                                                row.getWindowStart(),
                                                row.getWindowEnd(),
                                                row.getSampleCount(),
                                                row.getMachineState(),
                                                row.isScored(),
                                                row.getAnomalyScore(),
                                                row.getScoreThreshold(),
                                                row.getAnomaly(),
                                                row.getTemperatureCMean(),
                                                row.getVibrationMmSMean(),
                                                row.getPressureBarMean(),
                                                row.getPowerKwMean(),
                                                row.getRotationRpmMean(),
                                                row.getNullRatio()))
                        .toList());
    }

    /**
     * The window length, read from the data rather than configured here.
     *
     * <p>The producer owns that value; restating it in this service would create
     * a second source of truth that drifts the first time the pipeline is
     * retuned.
     */
    private static int windowSeconds(List<TelemetryWindowEntity> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        TelemetryWindowEntity first = rows.get(0);
        return (int) Duration.between(first.getWindowStart(), first.getWindowEnd()).toSeconds();
    }
}
