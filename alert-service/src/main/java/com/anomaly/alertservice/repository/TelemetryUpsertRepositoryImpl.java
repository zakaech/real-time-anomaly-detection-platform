package com.anomaly.alertservice.repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * One window in, one row out, however many times it is delivered.
 *
 * <p>The conflict target is {@code (machine_id, window_start)} rather than a
 * generated key: that pair is what identifies a window, and it is the reason
 * repeated emissions of the same window collapse instead of accumulating.
 *
 * <p>Unlike the alert upsert there is no {@code RETURNING (xmax = 0)} here.
 * Nothing downstream needs to know whether this particular write created the
 * row: telemetry is a curve, not an event, and no notification is derived from
 * it.
 */
@Repository
class TelemetryUpsertRepositoryImpl implements TelemetryUpsertRepository {

    private static final String SQL =
            """
            INSERT INTO telemetry_window (
                machine_id, window_start, window_end, sample_count, machine_state,
                is_scored, anomaly_score, score_threshold, is_anomaly,
                temperature_c_mean, vibration_mm_s_mean, pressure_bar_mean,
                power_kw_mean, rotation_rpm_mean, null_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (machine_id, window_start) DO UPDATE
               SET window_end          = EXCLUDED.window_end,
                   sample_count        = EXCLUDED.sample_count,
                   machine_state       = EXCLUDED.machine_state,
                   is_scored           = EXCLUDED.is_scored,
                   anomaly_score       = EXCLUDED.anomaly_score,
                   score_threshold     = EXCLUDED.score_threshold,
                   is_anomaly          = EXCLUDED.is_anomaly,
                   temperature_c_mean  = EXCLUDED.temperature_c_mean,
                   vibration_mm_s_mean = EXCLUDED.vibration_mm_s_mean,
                   pressure_bar_mean   = EXCLUDED.pressure_bar_mean,
                   power_kw_mean       = EXCLUDED.power_kw_mean,
                   rotation_rpm_mean   = EXCLUDED.rotation_rpm_mean,
                   null_ratio          = EXCLUDED.null_ratio,
                   updated_at          = now()
            """;

    private final JdbcTemplate jdbc;

    TelemetryUpsertRepositoryImpl(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void upsert(
            Long machineId,
            Instant windowStart,
            Instant windowEnd,
            int sampleCount,
            String machineState,
            boolean scored,
            Double anomalyScore,
            Double scoreThreshold,
            Boolean anomaly,
            Double temperatureCMean,
            Double vibrationMmSMean,
            Double pressureBarMean,
            Double powerKwMean,
            Double rotationRpmMean,
            Double nullRatio) {

        jdbc.update(
                SQL,
                (PreparedStatement ps) -> {
                    int i = 1;
                    ps.setLong(i++, machineId);
                    ps.setObject(i++, windowStart.atOffset(ZoneOffset.UTC));
                    ps.setObject(i++, windowEnd.atOffset(ZoneOffset.UTC));
                    ps.setInt(i++, sampleCount);
                    ps.setString(i++, machineState);
                    ps.setBoolean(i++, scored);
                    setNullableDouble(ps, i++, anomalyScore);
                    setNullableDouble(ps, i++, scoreThreshold);
                    if (anomaly == null) {
                        ps.setNull(i++, Types.BOOLEAN);
                    } else {
                        ps.setBoolean(i++, anomaly);
                    }
                    setNullableDouble(ps, i++, temperatureCMean);
                    setNullableDouble(ps, i++, vibrationMmSMean);
                    setNullableDouble(ps, i++, pressureBarMean);
                    setNullableDouble(ps, i++, powerKwMean);
                    setNullableDouble(ps, i++, rotationRpmMean);
                    setNullableDouble(ps, i, nullRatio);
                });
    }

    /**
     * A failed sensor produces a null aggregate, not a zero. Writing 0.0 would
     * put a plausible reading on the chart where there was none.
     */
    private static void setNullableDouble(PreparedStatement ps, int index, Double value)
            throws SQLException {
        if (value == null || value.isNaN() || value.isInfinite()) {
            ps.setNull(index, Types.DOUBLE);
        } else {
            ps.setDouble(index, value);
        }
    }
}
