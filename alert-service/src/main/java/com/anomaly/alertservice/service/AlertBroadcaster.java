package com.anomaly.alertservice.service;

import com.anomaly.alertservice.config.AlertServiceProperties;
import com.anomaly.alertservice.dto.AlertStreamEvent;
import com.anomaly.alertservice.entity.AlertEntity;
import com.anomaly.alertservice.entity.AlertSeverity;
import com.anomaly.alertservice.mapper.AlertMapper;
import com.anomaly.alertservice.repository.AlertRepository;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Fans newly created alerts out to connected dashboards.
 *
 * <p><strong>This is not a persistence mechanism.</strong> A disconnected client
 * loses nothing: the alert is in PostgreSQL and is reachable through
 * {@code GET /api/v1/alerts}. The stream only spares the dashboard from polling.
 * A client that connects after an alert was raised will not receive it here --
 * it sees it in the list, which is the normal initial load.
 *
 * <p>Two properties are load-bearing:
 *
 * <ul>
 *   <li>It reacts <strong>after commit</strong>. Emitting inside the transaction
 *       would let a client see an alert that is then rolled back, and no
 *       reconnection would ever correct that.
 *   <li>It never runs on the Kafka consumer thread. Writing to a slow client
 *       from there would stall ingestion behind a browser.
 * </ul>
 */
@Component
public class AlertBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(AlertBroadcaster.class);

    /** One alert was really inserted. Raised inside the transaction, delivered after it commits. */
    public record AlertCreatedEvent(UUID alertId) {}

    /**
     * An operator changed one alert. Raised inside the transaction, delivered
     * after it commits -- the same shape as {@link AlertCreatedEvent}, and for
     * the same reason, plus one learned the hard way: broadcasting from inside
     * the operator's transaction let a dead browser connection roll back a
     * committed-looking acknowledgement and answer 500. The display channel
     * must never be able to undo an operator action.
     */
    public record AlertUpdatedEvent(UUID alertId) {}

    private final AlertRepository alerts;
    private final AlertMapper mapper;
    private final IngestionMetrics metrics;
    private final AlertServiceProperties properties;

    private final Map<String, Subscriber> subscribers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "sse-heartbeat");
                        thread.setDaemon(true);
                        return thread;
                    });

    public AlertBroadcaster(
            AlertRepository alerts,
            AlertMapper mapper,
            IngestionMetrics metrics,
            AlertServiceProperties properties) {
        this.alerts = alerts;
        this.mapper = mapper;
        this.metrics = metrics;
        this.properties = properties;
        long interval = properties.sse().heartbeatIntervalMs();
        this.scheduler.scheduleAtFixedRate(
                this::heartbeat, interval, interval, TimeUnit.MILLISECONDS);
    }

    /** One connected dashboard, with the filters it asked for. */
    private record Subscriber(SseEmitter emitter, Set<AlertSeverity> severities, String lineCode) {

        boolean accepts(AlertStreamEvent event) {
            if (severities != null && !severities.isEmpty() && !severities.contains(event.severity())) {
                return false;
            }
            return lineCode == null || lineCode.equals(event.lineCode());
        }
    }

    /**
     * Register a client.
     *
     * @param lastEventId the {@code Last-Event-ID} header a reconnecting
     *     EventSource sends back, or null. Replay is bounded: a client that has
     *     been away for a long time gets the most recent window and is expected
     *     to reload the list rather than receive hours of history down a pipe.
     */
    public SseEmitter subscribe(Set<AlertSeverity> severities, String lineCode, Long lastEventId) {
        // No timeout: the connection is meant to stay open, and the heartbeat is
        // what detects a dead peer.
        return subscribe(new SseEmitter(0L), severities, lineCode, lastEventId);
    }

    /**
     * Register a client on a caller-supplied emitter.
     *
     * <p>This seam exists for one test. A peer whose connection the container
     * has already declared dead cannot be manufactured through MockMvc, and
     * that peer is exactly the case that once turned an acknowledgement into a
     * 500 and silenced the heartbeat. Production always goes through the
     * three-argument overload.
     */
    public SseEmitter subscribe(
            SseEmitter emitter, Set<AlertSeverity> severities, String lineCode, Long lastEventId) {
        String id = UUID.randomUUID().toString();
        Subscriber subscriber = new Subscriber(emitter, severities, lineCode);
        subscribers.put(id, subscriber);
        metrics.sseClientOpened();

        emitter.onCompletion(() -> remove(id));
        emitter.onTimeout(() -> remove(id));
        emitter.onError(throwable -> remove(id));

        if (lastEventId != null) {
            replayMissed(id, subscriber, lastEventId);
        }
        log.info("sse_client_connected clients={} last_event_id={}", metrics.currentSseClients(), lastEventId);
        return emitter;
    }

    private void replayMissed(String id, Subscriber subscriber, long lastEventId) {
        List<AlertEntity> missed =
                alerts.findAfterEventSeq(
                        lastEventId, PageRequest.of(0, properties.sse().maxReplayEvents()));
        for (AlertEntity alert : missed) {
            send(id, subscriber, "alert.created", mapper.toStreamEvent(alert));
        }
        if (!missed.isEmpty()) {
            log.info("sse_replayed events={} after_seq={}", missed.size(), lastEventId);
        }
    }

    /**
     * Delivered only once the ingestion transaction has committed, and only for
     * an alert that was genuinely inserted -- never for a duplicate.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onAlertCreated(AlertCreatedEvent event) {
        alerts.findByIdWithMachine(event.alertId())
                .map(mapper::toStreamEvent)
                .ifPresent(streamEvent -> broadcast("alert.created", streamEvent));
    }

    /**
     * Delivered only once the operator's transaction has committed. The row is
     * re-read here rather than carried in the event, so what the dashboards
     * receive is what the database holds -- never an in-flight entity that a
     * later rollback could have contradicted.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onAlertUpdated(AlertUpdatedEvent event) {
        alerts.findByIdWithMachine(event.alertId())
                .map(mapper::toStreamEvent)
                .ifPresent(streamEvent -> broadcast("alert.updated", streamEvent));
    }

    private void broadcast(String eventName, AlertStreamEvent event) {
        subscribers.forEach(
                (id, subscriber) -> {
                    if (subscriber.accepts(event)) {
                        send(id, subscriber, eventName, event);
                    }
                });
    }

    private void send(String id, Subscriber subscriber, String eventName, AlertStreamEvent event) {
        try {
            subscriber
                    .emitter()
                    .send(
                            SseEmitter.event()
                                    // The event id is the monotonic sequence, not the
                                    // alert UUID: a random identifier cannot serve as a
                                    // resumption cursor.
                                    .id(Long.toString(event.eventSeq()))
                                    .name(eventName)
                                    .data(event));
        } catch (IOException | IllegalStateException e) {
            // A client that cannot be written to is gone, or too slow to keep
            // up. Dropping it is the backpressure policy: it reconnects and
            // catches up through Last-Event-ID, which is cheaper than buffering
            // for a peer that may never read again.
            evict(id, subscriber, e);
        }
    }

    /**
     * Drop a peer that can no longer be written to.
     *
     * <p>The order is the point. The map entry goes <strong>first</strong>, so
     * whatever happens next the peer is never tried again -- and it was the
     * retrying that hurt: every later broadcast and every heartbeat walked into
     * the same dead connection.
     *
     * <p>Then {@code completeWithError}, guarded. On Tomcat, once the container
     * has already declared the async request failed, touching it again throws
     * {@code IllegalStateException} ("a non-container thread attempted to use
     * the AsyncContext after an error had occurred"). Observed on the running
     * stack: that exception escaped from here into the operator's
     * acknowledgement, rolled back its transaction and answered 500; thrown
     * from the heartbeat thread it silently cancelled every later tick. A peer
     * the container has given up on needs no farewell from us.
     */
    private void evict(String id, Subscriber subscriber, Exception cause) {
        remove(id);
        try {
            subscriber.emitter().completeWithError(cause);
        } catch (IllegalStateException alreadyDead) {
            log.debug("sse_peer_already_closed reason={}", alreadyDead.getMessage());
        }
    }

    /**
     * One heartbeat tick. Scheduled in production; invoked directly by the test
     * that proves a dead peer cannot stop it.
     *
     * <p>Nothing may escape this method. {@code scheduleAtFixedRate} cancels
     * every later run of a task that throws, and it does so silently -- the
     * failure mode is a stream that looks connected and never proves it again.
     */
    public void heartbeat() {
        try {
            subscribers.forEach(
                    (id, subscriber) -> {
                        try {
                            subscriber
                                    .emitter()
                                    .send(SseEmitter.event().name("heartbeat").data("{}"));
                        } catch (IOException | IllegalStateException e) {
                            evict(id, subscriber, e);
                        }
                    });
        } catch (RuntimeException e) {
            log.error("sse_heartbeat_failed clients={}", subscribers.size(), e);
        }
    }

    private void remove(String id) {
        if (subscribers.remove(id) != null) {
            metrics.sseClientClosed();
            log.info("sse_client_disconnected clients={}", metrics.currentSseClients());
        }
    }

    public int connectedClients() {
        return subscribers.size();
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
        subscribers.values().forEach(subscriber -> subscriber.emitter().complete());
        subscribers.clear();
    }
}
