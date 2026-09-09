package com.anomaly.alertservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.anomaly.alertservice.AlertMessages;
import com.anomaly.alertservice.PostgresTestBase;
import com.anomaly.alertservice.dto.AcknowledgeRequest;
import com.anomaly.alertservice.service.AlertBroadcaster;
import com.anomaly.alertservice.service.AlertIngestionService;
import com.anomaly.alertservice.service.AlertLifecycleService;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * The live stream, exercised over HTTP.
 *
 * <p>Going through MockMvc rather than calling the broadcaster directly is
 * deliberate: it is the only way to see what a browser would actually receive,
 * including the {@code event:} and {@code id:} lines that EventSource depends on
 * for dispatch and for reconnection.
 *
 * <p>Two properties matter more than the plumbing:
 *
 * <ul>
 *   <li>an event fires for a <strong>creation</strong> and not for a duplicate
 *       delivery, otherwise a Kafka replay makes the dashboard blink;
 *   <li>the stream is <strong>not persistence</strong> -- a client that was away
 *       loses nothing, because the alert is in PostgreSQL and reachable over
 *       REST. That is why the replay on reconnect is bounded rather than
 *       complete.
 * </ul>
 */
@AutoConfigureMockMvc
class AlertStreamTest extends PostgresTestBase {

    @Autowired MockMvc mvc;
    @Autowired AlertBroadcaster broadcaster;
    @Autowired AlertIngestionService ingestion;
    @Autowired AlertLifecycleService lifecycle;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM alert_acknowledgement");
        jdbc.update("DELETE FROM alert");
    }

    private MvcResult openStream(String query) throws Exception {
        MvcResult result =
                mvc.perform(get("/api/v1/alerts/stream" + query).accept(MediaType.TEXT_EVENT_STREAM))
                        .andExpect(request().asyncStarted())
                        .andReturn();
        // The connection stays open; the response accumulates as events arrive.
        return result;
    }

    private static String body(MvcResult result) throws UnsupportedEncodingException {
        return result.getResponse().getContentAsString();
    }

    @Test
    @DisplayName("a client connects and is counted, then removed when the connection ends")
    void clientLifecycleIsTracked() throws Exception {
        int before = broadcaster.connectedClients();

        MvcResult stream = openStream("");
        assertThat(broadcaster.connectedClients()).isEqualTo(before + 1);

        // The completion callback is fired by the servlet container, not by
        // calling complete() on the emitter, so the container is what has to be
        // driven here. Skipping this would leave the cleanup path untested and a
        // leak of dead connections invisible.
        stream.getRequest().getAsyncContext().complete();

        assertThat(broadcaster.connectedClients()).isEqualTo(before);
    }

    @Test
    @DisplayName("a newly created alert reaches the client with an ordered event id")
    void creationIsBroadcast() throws Exception {
        MvcResult stream = openStream("");

        ingestion.ingest(AlertMessages.valid());

        String received = body(stream);
        assertThat(received).contains("event:alert.created");
        assertThat(received).contains(AlertMessages.REAL_ALERT_ID);
        assertThat(received).contains("\"machineCode\":\"M-011\"");
        // The id line is what EventSource echoes back as Last-Event-ID, so it
        // has to be the monotonic sequence and not the random alert UUID.
        Long seq = jdbc.queryForObject("SELECT event_seq FROM alert", Long.class);
        assertThat(received).contains("id:" + seq);
    }

    @Test
    @DisplayName("a replayed delivery produces no second event")
    void duplicateDeliveryIsNotBroadcast() throws Exception {
        MvcResult stream = openStream("");

        ingestion.ingest(AlertMessages.valid());
        ingestion.ingest(AlertMessages.valid());

        // Two deliveries, one alert, one event. Broadcasting per delivery would
        // turn every Kafka replay into visible flicker.
        assertThat(countOccurrences(body(stream), "event:alert.created")).isEqualTo(1);
    }

    @Test
    @DisplayName("filtering happens on the server, not in the browser")
    void filtersAreAppliedOnTheServer() throws Exception {
        MvcResult lineB = openStream("?lineCode=LINE-B");
        MvcResult lineC = openStream("?lineCode=LINE-C");

        // M-011 is on LINE-C.
        ingestion.ingest(AlertMessages.valid());

        assertThat(body(lineB)).doesNotContain("alert.created");
        assertThat(body(lineC)).contains("alert.created");
    }

    @Test
    @DisplayName("a reconnecting client replays only what it missed")
    void reconnectionReplaysOnlyLaterEvents() throws Exception {
        ingestion.ingest(AlertMessages.valid());
        ingestion.ingest(
                AlertMessages.valid(
                        "55555555-5555-5555-8555-555555555555",
                        "M-003",
                        Instant.parse("2026-09-08T02:10:00Z"),
                        Instant.parse("2026-09-08T02:11:00Z")));

        Long firstSeq = jdbc.queryForObject("SELECT min(event_seq) FROM alert", Long.class);

        MvcResult reconnecting =
                mvc.perform(
                                get("/api/v1/alerts/stream")
                                        .accept(MediaType.TEXT_EVENT_STREAM)
                                        .header("Last-Event-ID", Long.toString(firstSeq)))
                        .andExpect(request().asyncStarted())
                        .andReturn();

        String received = body(reconnecting);
        // Only what came after the last id the client saw.
        assertThat(received).contains("M-003");
        assertThat(received).doesNotContain("M-011");
    }

    @Test
    @DisplayName("a malformed Last-Event-ID starts from now instead of failing")
    void malformedLastEventIdIsTolerated() throws Exception {
        ingestion.ingest(AlertMessages.valid());

        // Refusing the connection would leave the dashboard permanently blank
        // over one bad header.
        mvc.perform(
                        get("/api/v1/alerts/stream")
                                .accept(MediaType.TEXT_EVENT_STREAM)
                                .header("Last-Event-ID", "not-a-number"))
                .andExpect(status().isOk())
                .andExpect(request().asyncStarted());
    }

    @Test
    @DisplayName("an operator action is pushed as an update, not a creation")
    void acknowledgementIsBroadcastAsAnUpdate() throws Exception {
        ingestion.ingest(AlertMessages.valid());
        MvcResult stream = openStream("");

        lifecycle.acknowledge(
                UUID.fromString(AlertMessages.REAL_ALERT_ID),
                new AcknowledgeRequest("looked at it", null),
                "op.martin");

        String received = body(stream);
        assertThat(received).contains("event:alert.updated");
        assertThat(received).contains("ACKNOWLEDGED");
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int index = haystack.indexOf(needle);
        while (index >= 0) {
            count++;
            index = haystack.indexOf(needle, index + needle.length());
        }
        return count;
    }
}
