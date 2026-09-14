import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { Subject } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { APP_CONFIG, FALLBACK_CONFIG } from '../config/app-config';
import { AlertSummary } from '../models/alert.model';
import { AlertStreamEvent } from '../models/stream-event.model';
import { AlertStreamService, StreamMessage } from '../sse/alert-stream.service';
import { AlertStoreService } from './alert-store.service';

/**
 * Deduplication is the property this file exists for.
 *
 * A crash test measured 8 duplicate alert deliveries (docs/10), and the SSE
 * channel replays up to 500 events on reconnection. A feed that appended rows
 * would show the same alert several times and its counters would drift and
 * never recover.
 */
describe('AlertStoreService', () => {
  const events$ = new Subject<StreamMessage>();
  const connections$ = new Subject<void>();
  let http: HttpTestingController;
  let store: AlertStoreService;

  const streamStub = {
    events: events$.asObservable(),
    connections: connections$.asObservable(),
    state: () => 'connected' as const,
    connect: vi.fn(),
    disconnect: vi.fn(),
  };

  const alert = (id: string, overrides: Partial<AlertSummary> = {}): AlertSummary => ({
    alertId: id,
    machineCode: 'M-011',
    lineCode: 'LINE-C',
    severity: 'CRITICAL',
    status: 'NEW',
    anomalyScore: 0.999,
    scoreThreshold: 0.9927,
    detectedAt: '2026-09-09T10:00:00Z',
    consecutiveWindows: 2,
    ...overrides,
  });

  const streamed = (id: string, overrides: Partial<AlertStreamEvent> = {}): AlertStreamEvent => ({
    alertId: id,
    eventSeq: 1,
    machineCode: 'M-011',
    lineCode: 'LINE-C',
    severity: 'CRITICAL',
    status: 'NEW',
    anomalyScore: 0.999,
    detectedAt: '2026-09-09T10:00:00Z',
    ...overrides,
  });

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: APP_CONFIG, useValue: FALLBACK_CONFIG },
        { provide: AlertStreamService, useValue: streamStub },
      ],
    });
    http = TestBed.inject(HttpTestingController);
    store = TestBed.inject(AlertStoreService);
  });

  afterEach(() => vi.clearAllMocks());

  /** Answer the initial REST load the store issues on start(). */
  function flushInitialLoad(content: AlertSummary[]): void {
    const request = http.expectOne((r) => r.url === '/api/v1/alerts');
    request.flush({
      content,
      page: 0,
      size: 100,
      totalElements: content.length,
      totalPages: 1,
      last: true,
    });
  }

  it('loads the current state before following the stream', () => {
    store.start();
    flushInitialLoad([alert('a'), alert('b')]);

    expect(store.total()).toBe(2);
    expect(streamStub.connect).toHaveBeenCalled();
  });

  it('adds a streamed alert to the feed', () => {
    store.start();
    flushInitialLoad([]);

    events$.next({ name: 'alert.created', event: streamed('a') });

    expect(store.total()).toBe(1);
    expect(store.feed()[0].alertId).toBe('a');
  });

  it('the same event delivered twice creates one entry', () => {
    store.start();
    flushInitialLoad([]);

    events$.next({ name: 'alert.created', event: streamed('a') });
    events$.next({ name: 'alert.created', event: streamed('a') });

    expect(store.total()).toBe(1);
    expect(store.feed()).toHaveLength(1);
  });

  it('an alert already loaded over REST is not duplicated by the stream', () => {
    store.start();
    flushInitialLoad([alert('a')]);

    // The overlap between the initial load and the live channel: the same alert
    // arrives by both routes.
    events$.next({ name: 'alert.created', event: streamed('a') });

    expect(store.total()).toBe(1);
  });

  it('a Last-Event-ID replay of many events creates no duplicates', () => {
    store.start();
    flushInitialLoad([alert('a'), alert('b')]);

    // What a reconnection looks like: the backend replays what we already have.
    for (let i = 0; i < 10; i++) {
      events$.next({ name: 'alert.created', event: streamed('a', { eventSeq: i + 1 }) });
      events$.next({ name: 'alert.created', event: streamed('b', { eventSeq: i + 20 }) });
    }

    expect(store.total()).toBe(2);
  });

  it('an update replaces the row instead of adding one', () => {
    store.start();
    flushInitialLoad([alert('a')]);

    events$.next({
      name: 'alert.updated',
      event: streamed('a', { status: 'ACKNOWLEDGED' }),
    });

    expect(store.total()).toBe(1);
    expect(store.feed()[0].status).toBe('ACKNOWLEDGED');
    expect(store.newCount()).toBe(0);
  });

  it('counters stay consistent after duplicates', () => {
    store.start();
    flushInitialLoad([]);

    events$.next({ name: 'alert.created', event: streamed('a', { severity: 'CRITICAL' }) });
    events$.next({ name: 'alert.created', event: streamed('a', { severity: 'CRITICAL' }) });
    events$.next({ name: 'alert.created', event: streamed('b', { severity: 'HIGH' }) });

    // Derived from the map, so a duplicate cannot inflate them -- which a
    // hand-incremented counter would.
    expect(store.countsBySeverity()).toEqual({ CRITICAL: 1, HIGH: 1, MEDIUM: 0 });
    expect(store.total()).toBe(2);
  });

  it('tracks the highest event sequence seen', () => {
    store.start();
    flushInitialLoad([]);

    events$.next({ name: 'alert.created', event: streamed('a', { eventSeq: 7 }) });
    events$.next({ name: 'alert.created', event: streamed('b', { eventSeq: 3 }) });

    // Monotonic: an out-of-order replay must not move the cursor backwards.
    expect(store.lastEventSeq()).toBe(7);
  });

  it('resynchronises over REST whenever the stream reconnects', () => {
    store.start();
    flushInitialLoad([alert('a')]);

    connections$.next();

    // The REST reload is what closes a gap longer than the backend replay bound.
    const request = http.expectOne((r) => r.url === '/api/v1/alerts');
    request.flush({
      content: [alert('a'), alert('b')],
      page: 0,
      size: 100,
      totalElements: 2,
      totalPages: 1,
      last: true,
    });

    expect(store.total()).toBe(2);
  });

  it('a REST reload replaces the state rather than merging it', () => {
    store.start();
    flushInitialLoad([alert('a'), alert('b')]);

    connections$.next();
    http
      .expectOne((r) => r.url === '/api/v1/alerts')
      .flush({
        content: [alert('b')],
        page: 0,
        size: 100,
        totalElements: 1,
        totalPages: 1,
        last: true,
      });

    // The API is authoritative: keeping 'a' because we saw it once would leave
    // the dashboard showing an alert the database no longer returns.
    expect(store.total()).toBe(1);
    expect(store.feed()[0].alertId).toBe('b');
  });

  it('surfaces a load failure instead of showing an empty feed', () => {
    store.start();
    http
      .expectOne((r) => r.url === '/api/v1/alerts')
      .flush(
        { title: 'Internal error', status: 500, detail: 'boom' },
        { status: 500, statusText: 'Server Error' },
      );

    expect(store.error()).toBeTruthy();
    expect(store.loading()).toBe(false);
  });
});
