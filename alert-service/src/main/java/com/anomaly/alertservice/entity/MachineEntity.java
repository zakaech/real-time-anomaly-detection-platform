package com.anomaly.alertservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * A machine, identified by the code that appears on every alert.
 *
 * <p>The columns are only those something actually produces: {@code code} and
 * {@code lineCode} come from the alert contract, {@code machineType} from the
 * simulator roster via the Flyway seed. The initial design also proposed
 * criticality, a commissioning date and nominal ranges; nothing emits them, and
 * criticality in particular is described in the contract as feeding severity
 * while the stream-processor computes severity from score and threshold alone.
 * Creating them would have meant three columns that are always null.
 */
@Entity
@Table(name = "machine")
public class MachineEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "code", nullable = false, unique = true, length = 16)
    private String code;

    @Column(name = "line_code", nullable = false, length = 16)
    private String lineCode;

    /** Null for a machine auto-provisioned from an alert; the seed fills it. */
    @Column(name = "machine_type", length = 32)
    private String machineType;

    @Column(name = "first_seen_at", nullable = false, insertable = false, updatable = false)
    private Instant firstSeenAt;

    @Column(name = "is_active", nullable = false)
    private boolean active = true;

    protected MachineEntity() {
        // JPA
    }

    public Long getId() {
        return id;
    }

    public String getCode() {
        return code;
    }

    public String getLineCode() {
        return lineCode;
    }

    public String getMachineType() {
        return machineType;
    }

    public Instant getFirstSeenAt() {
        return firstSeenAt;
    }

    public boolean isActive() {
        return active;
    }
}
