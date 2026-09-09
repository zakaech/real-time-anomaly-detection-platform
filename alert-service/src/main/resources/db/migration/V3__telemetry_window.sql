-- Telemetry windows, persisted so the dashboard can draw a real sensor curve
-- (decision D-41).
--
-- WHY THIS TABLE EXISTS. telemetry.raw is never persisted and has 7 days of
-- Kafka retention; alert.features is always empty because the alerting query
-- does not parse the feature vector. Without this table the dashboard could
-- show a score and nothing else, and any sensor curve it drew would be invented.
--
-- WHAT IS STORED, AND WHY ONLY THIS. telemetry.scored carries 52 features per
-- window. Persisting all of them would be storing a training matrix to draw
-- five lines. Only the five sensor means are kept -- one per physical signal --
-- plus what is needed to mark anomalies on the curve and to tell a partial
-- window from a complete one. Nothing here is derived or computed: every column
-- is a value the message already carried.
--
-- VOLUME. Measured on a real run: the scoring query emitted 11 342 window
-- updates for 2 h of simulated telemetry across 15 machines, i.e. roughly
-- 5 700 rows/hour before deduplication. The unique constraint collapses the
-- repeated emissions of one window (update mode republishes a window as it
-- fills), and a scheduled purge bounds the history.

CREATE TABLE telemetry_window (
    id                  BIGSERIAL,
    machine_id          BIGINT           NOT NULL,
    window_start        TIMESTAMPTZ      NOT NULL,
    window_end          TIMESTAMPTZ      NOT NULL,

    -- How full the window was at the last emission. Update mode republishes a
    -- window as it fills, so this rises over successive writes; keeping it lets
    -- the dashboard show that a point is built on partial data instead of
    -- silently drawing it like any other.
    sample_count        INTEGER          NOT NULL,
    machine_state       VARCHAR(16)      NOT NULL,

    -- Null on a window the model did not score (too few samples, machine not
    -- running, window not yet mature). The sensor means below are still real
    -- and still worth plotting, which is why they are not in the same nullable
    -- group.
    is_scored           BOOLEAN          NOT NULL,
    anomaly_score       DOUBLE PRECISION,
    score_threshold     DOUBLE PRECISION,
    is_anomaly          BOOLEAN,

    -- One column per physical signal, straight from the message. Nullable
    -- because a failed sensor produces a null aggregate rather than a zero.
    temperature_c_mean  DOUBLE PRECISION,
    vibration_mm_s_mean DOUBLE PRECISION,
    pressure_bar_mean   DOUBLE PRECISION,
    power_kw_mean       DOUBLE PRECISION,
    rotation_rpm_mean   DOUBLE PRECISION,

    -- Share of missing sensor readings in the window. It is what explains a
    -- gap in a curve, so the dashboard can say "sensor down" rather than
    -- drawing a straight line through it.
    null_ratio          DOUBLE PRECISION,

    updated_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),

    CONSTRAINT pk_telemetry_window PRIMARY KEY (id),
    CONSTRAINT fk_telemetry_window_machine FOREIGN KEY (machine_id) REFERENCES machine (id),

    -- The idempotency key. telemetry.scored is written in update mode, so one
    -- window arrives many times -- 5.67 emissions per window on a live run,
    -- 1.04 on a backfill, both measured. Without this constraint the table
    -- would hold five copies of every point.
    CONSTRAINT uq_telemetry_window UNIQUE (machine_id, window_start),

    CONSTRAINT ck_telemetry_window_bounds CHECK (window_end > window_start),
    CONSTRAINT ck_telemetry_window_samples CHECK (sample_count >= 0),
    CONSTRAINT ck_telemetry_window_score CHECK (anomaly_score IS NULL OR (anomaly_score >= 0 AND anomaly_score <= 1))
);

-- The only query shape the dashboard issues: one machine, one time range,
-- chronological. Descending matches the alert indexes and serves both
-- directions equally.
CREATE INDEX idx_telemetry_window_machine_time
    ON telemetry_window (machine_id, window_start DESC);

-- Purging is by age across all machines, so it needs its own entry point.
CREATE INDEX idx_telemetry_window_start ON telemetry_window (window_start);
