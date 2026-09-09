package com.anomaly.alertservice.service;

import com.anomaly.alertservice.dto.AlertDetail;
import com.anomaly.alertservice.dto.AlertSummary;
import com.anomaly.alertservice.entity.AlertEntity;
import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.entity.AlertStatus;
import com.anomaly.alertservice.entity.MachineEntity;
import com.anomaly.alertservice.exception.AlertNotFoundException;
import com.anomaly.alertservice.mapper.AlertMapper;
import com.anomaly.alertservice.repository.AlertAcknowledgementRepository;
import com.anomaly.alertservice.repository.AlertRepository;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.JoinType;
import jakarta.persistence.criteria.Predicate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Read side. Every filter and every page boundary is resolved by PostgreSQL. */
@Service
public class AlertQueryService {

    private final AlertRepository alerts;
    private final AlertAcknowledgementRepository acknowledgements;
    private final AlertMapper mapper;

    public AlertQueryService(
            AlertRepository alerts,
            AlertAcknowledgementRepository acknowledgements,
            AlertMapper mapper) {
        this.alerts = alerts;
        this.acknowledgements = acknowledgements;
        this.mapper = mapper;
    }

    /**
     * One page of alerts.
     *
     * <p>The criteria below become a single SQL statement with WHERE, ORDER BY
     * and LIMIT/OFFSET. Nothing is filtered in Java: loading the table and
     * filtering it in memory would work at demo scale and collapse at any other.
     */
    @Transactional(readOnly = true)
    public Page<AlertSummary> search(AlertFilter filter, Pageable pageable) {
        return alerts.findAll(toSpecification(filter), pageable).map(mapper::toSummary);
    }

    @Transactional(readOnly = true)
    public AlertDetail detail(UUID alertId) {
        AlertEntity alert =
                alerts.findByIdWithMachine(alertId)
                        .orElseThrow(() -> new AlertNotFoundException(alertId));
        return mapper.toDetail(alert, acknowledgements.findByAlertIdOrderByOccurredAtDesc(alertId));
    }

    @SuppressWarnings("unchecked") // Hibernate's Fetch for a ManyToOne is a Join.
    static Specification<AlertEntity> toSpecification(AlertFilter filter) {
        return (root, query, cb) -> {
            // Spring Data issues two queries for a page: the rows, and a count.
            // A fetch join is illegal on the count query, so the join is a fetch
            // only for the row query. Without the fetch, mapping each row would
            // lazily load its machine and produce one query per row.
            boolean counting =
                    Long.class.equals(query.getResultType())
                            || long.class.equals(query.getResultType());
            // Fetch and Join are unrelated interfaces in the Jakarta API even
            // though Hibernate's implementation of a to-one fetch is a Join, so
            // the cast has to go through Object. The alternative -- joining and
            // fetching separately -- emits the same table twice.
            Join<AlertEntity, MachineEntity> machine =
                    counting
                            ? root.join("machine", JoinType.INNER)
                            : (Join<AlertEntity, MachineEntity>)
                                    (Object) root.fetch("machine", JoinType.INNER);

            List<Predicate> predicates = new ArrayList<>();
            if (filter.machineCode() != null) {
                predicates.add(cb.equal(machine.get("code"), filter.machineCode()));
            }
            if (filter.lineCode() != null) {
                predicates.add(cb.equal(machine.get("lineCode"), filter.lineCode()));
            }
            if (filter.severities() != null && !filter.severities().isEmpty()) {
                predicates.add(root.get("severity").in(filter.severities()));
            }
            if (filter.statuses() != null && !filter.statuses().isEmpty()) {
                predicates.add(root.get("status").in(filter.statuses()));
            }
            if (filter.from() != null) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("detectedAt"), filter.from()));
            }
            if (filter.to() != null) {
                predicates.add(cb.lessThanOrEqualTo(root.get("detectedAt"), filter.to()));
            }
            return predicates.isEmpty() ? cb.conjunction() : cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    /** The filters the list endpoint accepts. All optional, all combinable. */
    public record AlertFilter(
            String machineCode,
            String lineCode,
            List<AlertSeverity> severities,
            List<AlertStatus> statuses,
            Instant from,
            Instant to) {}
}
