-- Phase 4 schema: machine, alert, alert_acknowledgement (decision D-38).
--
-- Flyway is the ONLY source of DDL. compose deliberately mounts no init SQL and
-- Hibernate runs with ddl-auto=validate, so there is exactly one place where the
-- shape of this database is defined.
--
-- Every column here is justified by a field that actually exists in
-- contracts/json-schema/alert.v1.json, or is technical. Columns invented in the
-- Phase 0 sketch (criticality, commissioned_on, nominal_ranges) are deliberately
-- absent: nothing produces them, and `criticality` in particular is described in
-- the contract as feeding severity while the stream-processor computes severity
-- from score and threshold alone.

CREATE TABLE machine (
    id            BIGSERIAL,
    code          VARCHAR(16)  NOT NULL,
    line_code     VARCHAR(16)  NOT NULL,
    -- Known only from the simulator roster, so it is seeded, never derived from
    -- an alert. An auto-provisioned machine has it null until a seed fills it.
    machine_type  VARCHAR(32),
    first_seen_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_machine PRIMARY KEY (id),
    CONSTRAINT uq_machine_code UNIQUE (code),
    CONSTRAINT ck_machine_code CHECK (code ~ '^M-[0-9]{3}$'),
    CONSTRAINT ck_machine_line CHECK (line_code ~ '^LINE-[A-Z]$')
);

CREATE INDEX idx_machine_line_code ON machine (line_code);

CREATE TABLE alert (
    -- The alert_id from the contract IS the primary key (decision: option A).
    -- It is a deterministic UUIDv5 of (machine_id, window_start, model_name,
    -- model_version), so it already is the natural key in scalar form, and it is
    -- the identifier the REST API exposes. A surrogate key would add a second
    -- identity that no client ever sees.
    id                    UUID          NOT NULL,
    machine_id            BIGINT        NOT NULL,
    status                VARCHAR(16)   NOT NULL DEFAULT 'NEW',
    severity              VARCHAR(16)   NOT NULL,
    anomaly_score         DOUBLE PRECISION NOT NULL,
    score_threshold       DOUBLE PRECISION NOT NULL,
    detected_at           TIMESTAMPTZ   NOT NULL,
    window_start          TIMESTAMPTZ   NOT NULL,
    window_end            TIMESTAMPTZ   NOT NULL,
    published_at          TIMESTAMPTZ   NOT NULL,
    consecutive_windows   INTEGER       NOT NULL DEFAULT 1,
    model_name            VARCHAR(64)   NOT NULL,
    model_version         VARCHAR(32)   NOT NULL,
    model_trained_at      TIMESTAMPTZ,
    model_artifact_sha256 VARCHAR(64),
    top_contributors      JSONB         NOT NULL DEFAULT '[]'::jsonb,
    -- Declared by the contract but currently always empty: the alerting query
    -- never parses the 52-feature vector. The column exists so that carrying it
    -- later needs no migration, and it is documented as empty rather than
    -- presented as available.
    features              JSONB         NOT NULL DEFAULT '{}'::jsonb,
    -- Monotonic publication order, for SSE Last-Event-ID. The primary key is a
    -- random UUID and cannot order anything, so resuming a stream needs this.
    event_seq             BIGSERIAL     NOT NULL,
    ingested_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ   NOT NULL DEFAULT now(),
    optlock_version       BIGINT        NOT NULL DEFAULT 0,

    CONSTRAINT pk_alert PRIMARY KEY (id),
    CONSTRAINT fk_alert_machine FOREIGN KEY (machine_id) REFERENCES machine (id),
    CONSTRAINT uq_alert_event_seq UNIQUE (event_seq),

    -- Defence in depth, not decoration. If the upstream derivation of alert_id
    -- ever changed, the same window would arrive under a new UUID and the
    -- primary key would happily insert a duplicate. This constraint turns that
    -- silent duplication into a loud failure.
    CONSTRAINT uq_alert_natural UNIQUE (machine_id, window_start, model_name, model_version),

    CONSTRAINT ck_alert_status CHECK (status IN ('NEW', 'ACKNOWLEDGED', 'RESOLVED', 'DISMISSED')),
    CONSTRAINT ck_alert_severity CHECK (severity IN ('MEDIUM', 'HIGH', 'CRITICAL')),
    CONSTRAINT ck_alert_score CHECK (anomaly_score >= 0 AND anomaly_score <= 1),
    CONSTRAINT ck_alert_threshold CHECK (score_threshold >= 0 AND score_threshold <= 1),
    CONSTRAINT ck_alert_window CHECK (window_end > window_start),
    CONSTRAINT ck_alert_consecutive CHECK (consecutive_windows >= 1),
    -- The contract requires detected_at = window_end and the producer validates
    -- it. Restating it here means a producer bug cannot quietly land a row that
    -- shows an operator the wrong instant.
    CONSTRAINT ck_alert_detected_at CHECK (detected_at = window_end)
);

-- Default listing order, and any date-range filter.
CREATE INDEX idx_alert_detected_at ON alert (detected_at DESC);
-- Filter by machine, still ordered by time.
CREATE INDEX idx_alert_machine_time ON alert (machine_id, detected_at DESC);
-- Filter by severity, still ordered by time.
CREATE INDEX idx_alert_severity_time ON alert (severity, detected_at DESC);
-- Partial index: the operator queue is the dominant query and touches only the
-- fraction of rows still NEW, so the index stays small as history grows.
CREATE INDEX idx_alert_open ON alert (detected_at DESC) WHERE status = 'NEW';
-- Reconnecting SSE clients replay forward from their last seen sequence.
CREATE INDEX idx_alert_event_seq ON alert (event_seq);

CREATE TABLE alert_acknowledgement (
    id              BIGSERIAL,
    alert_id        UUID         NOT NULL,
    previous_status VARCHAR(16)  NOT NULL,
    new_status      VARCHAR(16)  NOT NULL,
    actor           VARCHAR(128) NOT NULL,
    comment         TEXT,
    occurred_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_alert_ack PRIMARY KEY (id),
    CONSTRAINT fk_ack_alert FOREIGN KEY (alert_id) REFERENCES alert (id) ON DELETE CASCADE,
    CONSTRAINT ck_ack_new_status CHECK (new_status IN ('ACKNOWLEDGED', 'RESOLVED', 'DISMISSED'))
);

-- Append-only audit trail: the history of one alert, most recent first.
CREATE INDEX idx_ack_alert ON alert_acknowledgement (alert_id, occurred_at DESC);
