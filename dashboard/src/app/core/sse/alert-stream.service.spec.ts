import { TestBed } from '@angular/core/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { APP_CONFIG, FALLBACK_CONFIG } from '../config/app-config';
import { StreamMessage } from './alert-stream.service';
import { AlertStreamService } from './alert-stream.service';

/**
 * A controllable EventSource.
 *
 * The real one cannot be driven from a test -- it opens a network connection and
 * reconnects on the browser's own schedule -- so it is replaced. Everything
 * asserted below is behaviour of our service, not of the double.
 */
class FakeEventSource {
  static instances: FakeEventSource[] = [];

  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;
  readonly listeners = new Map<string, ((event: MessageEvent) => void)[]>();

  constructor(readonly url: string) {
    FakeEventSource.instances.push(this);
  }

  addEventListener(name: string, handler: (event: MessageEvent) => void): void {
    const existing = this.listeners.get(name) ?? [];
    existing.push(handler);
    this.listeners.set(name, existing);
  }

  close(): void {
    this.closed = true;
  }

  /** Simulate the server pushing a named event. */
  emit(name: string, data: unknown): void {
    for (const handler of this.listeners.get(name) ?? []) {
      handler({ data: JSON.stringify(data) } as MessageEvent);
    }
  }

  emitRaw(name: string, raw: string): void {
    for (const handler of this.listeners.get(name) ?? []) {
      handler({ data: raw } as MessageEvent);
    }
  }

  static reset(): void {
    FakeEventSource.instances = [];
  }

  static get latest(): FakeEventSource {
    return FakeEventSource.instances[FakeEventSource.instances.length - 1];
  }
}

describe('AlertStreamService', () => {
  let service: AlertStreamService;

  beforeEach(() => {
    FakeEventSource.reset();
    vi.stubGlobal('EventSource', FakeEventSource);
    vi.useFakeTimers();

    TestBed.configureTestingModule({
      providers: [{ provide: APP_CONFIG, useValue: FALLBACK_CONFIG }],
    });
    service = TestBed.inject(AlertStreamService);
  });

  afterEach(() => {
    service.disconnect();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('opens the stream endpoint and reports connected', () => {
    service.connect();
    expect(service.state()).toBe('connecting');

    FakeEventSource.latest.onopen?.();

    expect(FakeEventSource.latest.url).toBe('/api/v1/alerts/stream');
    expect(service.state()).toBe('connected');
  });

  it('forwards alert.created with a parsed payload', () => {
    const received: StreamMessage[] = [];
    service.events.subscribe((message) => received.push(message));

    service.connect();
    FakeEventSource.latest.onopen?.();
    FakeEventSource.latest.emit('alert.created', { alertId: 'a', eventSeq: 3 });

    expect(received).toHaveLength(1);
    expect(received[0].name).toBe('alert.created');
    expect(received[0].event.alertId).toBe('a');
    expect(received[0].event.eventSeq).toBe(3);
  });

  it('forwards alert.updated', () => {
    const received: StreamMessage[] = [];
    service.events.subscribe((message) => received.push(message));

    service.connect();
    FakeEventSource.latest.onopen?.();
    FakeEventSource.latest.emit('alert.updated', { alertId: 'a', status: 'ACKNOWLEDGED' });

    expect(received[0].name).toBe('alert.updated');
  });

  it('ignores heartbeats entirely', () => {
    const received: StreamMessage[] = [];
    service.events.subscribe((message) => received.push(message));

    service.connect();
    FakeEventSource.latest.onopen?.();
    FakeEventSource.latest.emit('heartbeat', {});

    // A heartbeat exists to stop a proxy closing an idle connection. Forwarding
    // it would make every listener redraw every 15 seconds for nothing.
    expect(received).toHaveLength(0);
    expect(service.state()).toBe('connected');
  });

  it('survives a frame it cannot parse', () => {
    const received: StreamMessage[] = [];
    service.events.subscribe((message) => received.push(message));

    service.connect();
    FakeEventSource.latest.onopen?.();
    FakeEventSource.latest.emitRaw('alert.created', 'not json');

    // One bad frame must not tear down a working stream; the REST
    // resynchronisation covers whatever it described.
    expect(received).toHaveLength(0);
    expect(service.state()).toBe('connected');
  });

  it('closes the source on error and reports reconnecting', () => {
    service.connect();
    FakeEventSource.latest.onopen?.();
    const first = FakeEventSource.latest;

    first.onerror?.();

    // Closed on purpose: the browser would otherwise retry on its own schedule
    // and hammer a backend that is down.
    expect(first.closed).toBe(true);
    expect(service.state()).toBe('reconnecting');
  });

  it('reopens after the backoff delay', () => {
    service.connect();
    FakeEventSource.latest.onopen?.();
    expect(FakeEventSource.instances).toHaveLength(1);

    FakeEventSource.latest.onerror?.();
    vi.advanceTimersByTime(FALLBACK_CONFIG.sseInitialBackoffMs);

    expect(FakeEventSource.instances).toHaveLength(2);
  });

  it('grows the backoff on repeated failures and caps it', () => {
    service.connect();
    const delays: number[] = [];

    for (let attempt = 0; attempt < 8; attempt++) {
      delays.push(service.currentBackoffMs);
      FakeEventSource.latest.onerror?.();
      vi.advanceTimersByTime(FALLBACK_CONFIG.sseMaxBackoffMs);
    }

    // Doubling, so a backend that is down is not retried every second...
    expect(delays[1]).toBeGreaterThan(delays[0]);
    expect(delays[2]).toBeGreaterThan(delays[1]);
    // ...but bounded, so recovery is never further away than the cap.
    expect(service.currentBackoffMs).toBeLessThanOrEqual(FALLBACK_CONFIG.sseMaxBackoffMs);
  });

  it('resets the backoff once a connection succeeds', () => {
    service.connect();
    FakeEventSource.latest.onerror?.();
    vi.advanceTimersByTime(FALLBACK_CONFIG.sseInitialBackoffMs);
    FakeEventSource.latest.onerror?.();
    expect(service.currentBackoffMs).toBeGreaterThan(FALLBACK_CONFIG.sseInitialBackoffMs);

    vi.advanceTimersByTime(FALLBACK_CONFIG.sseMaxBackoffMs);
    FakeEventSource.latest.onopen?.();

    // A brief blip must not leave the next reconnection minutes away.
    expect(service.currentBackoffMs).toBe(FALLBACK_CONFIG.sseInitialBackoffMs);
  });

  it('signals every successful connection so callers can resynchronise', () => {
    let connections = 0;
    service.connections.subscribe(() => connections++);

    service.connect();
    FakeEventSource.latest.onopen?.();
    FakeEventSource.latest.onerror?.();
    vi.advanceTimersByTime(FALLBACK_CONFIG.sseInitialBackoffMs);
    FakeEventSource.latest.onopen?.();

    // Each reconnection triggers a REST reload, which is what covers a gap
    // longer than the backend replay bound.
    expect(connections).toBe(2);
  });

  it('an explicit disconnect stops reconnecting', () => {
    service.connect();
    FakeEventSource.latest.onopen?.();

    service.disconnect();
    const openedSoFar = FakeEventSource.instances.length;
    vi.advanceTimersByTime(FALLBACK_CONFIG.sseMaxBackoffMs * 3);

    expect(service.state()).toBe('disconnected');
    // Leaving the page must not leave a timer reopening connections.
    expect(FakeEventSource.instances).toHaveLength(openedSoFar);
  });

  it('connect is idempotent', () => {
    service.connect();
    service.connect();

    // A second call must not open a second connection against the server.
    expect(FakeEventSource.instances).toHaveLength(1);
  });
});
