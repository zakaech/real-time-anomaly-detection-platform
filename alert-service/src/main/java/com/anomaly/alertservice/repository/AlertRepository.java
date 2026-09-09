package com.anomaly.alertservice.repository;

import com.anomaly.alertservice.entity.AlertEntity;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * Reads go through JPA; the ingestion write does not.
 *
 * <p>The idempotent insert lives in {@link AlertUpsertRepository}, mixed in as a
 * custom fragment. Spring Data cannot express it: a {@code @Modifying} query may
 * only return void or a row count, and this one has to report whether the row
 * was inserted or merely updated.
 *
 * <p>Filtering and pagination are executed by PostgreSQL through
 * {@link JpaSpecificationExecutor} and {@code Pageable}, never by loading rows
 * and filtering them in Java.
 */
@Repository
public interface AlertRepository
        extends JpaRepository<AlertEntity, UUID>,
                JpaSpecificationExecutor<AlertEntity>,
                AlertUpsertRepository {

    /** Detail view: the machine is needed, so it is fetched in the same query. */
    @Query("SELECT a FROM AlertEntity a JOIN FETCH a.machine WHERE a.id = :id")
    Optional<AlertEntity> findByIdWithMachine(@Param("id") UUID id);

    /**
     * Forward replay for a reconnecting SSE client, bounded by the caller.
     *
     * <p>Ordered by the monotonic sequence rather than by time: two alerts can
     * share a detection instant, and a cursor has to be a total order.
     */
    @Query("SELECT a FROM AlertEntity a JOIN FETCH a.machine WHERE a.eventSeq > :after ORDER BY a.eventSeq ASC")
    List<AlertEntity> findAfterEventSeq(@Param("after") long after, org.springframework.data.domain.Pageable page);
}
