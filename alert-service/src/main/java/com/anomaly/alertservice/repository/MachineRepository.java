package com.anomaly.alertservice.repository;

import com.anomaly.alertservice.entity.MachineEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface MachineRepository extends JpaRepository<MachineEntity, Long> {

    Optional<MachineEntity> findByCode(String code);

    /**
     * Register a machine seen for the first time on an alert (decision D-39).
     *
     * <p>The alternative was a strict foreign key that rejects an alert whose
     * machine is missing from the reference table. That is the wrong failure
     * mode: it discards a real detection because a seed is out of date. An alert
     * carries machine_id and line_id, which is everything this row needs, so the
     * reference table is completed by the stream and enriched by the seed.
     *
     * <p>{@code DO NOTHING} rather than a check-then-insert: three consumer
     * threads can meet the same new machine at the same moment, and only the
     * database can arbitrate that atomically.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
            value =
                    """
                    INSERT INTO machine (code, line_code)
                    VALUES (:code, :lineCode)
                    ON CONFLICT (code) DO NOTHING
                    """,
            nativeQuery = true)
    void insertIfAbsent(@Param("code") String code, @Param("lineCode") String lineCode);
}
