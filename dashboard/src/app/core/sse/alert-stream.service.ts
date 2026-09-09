import { Injectable, NgZone, OnDestroy, computed, inject, signal } from '@angular/core';
import { Subject } from 'rxjs';
import { APP_CONFIG } from '../config/app-config';
import { AlertStreamEvent, ConnectionState } from '../models/stream-event.model';

/** An event, with the name that carried it. */
export interface StreamMessage {
  readonly name: 'alert.created' | 'alert.updated';
  readonly event: AlertStreamEvent;
}

/**
 * The live channel.
 *
 * ## Last-Event-ID, and what a browser will and will not do
 *
 * `EventSource` does not let an application set request headers. `Last-Event-ID`
 * is sent **by the browser**, **automatically**, and **only on an automatic
 * reconnection**, taken from the last `id:` line received. It cannot be supplied
 * on the first connection.
 *
 * That is fine here, and the reason is architectural rather than a workaround:
 * the stream is not the source of truth. PostgreSQL is. So the sequence is
 *
 *   1. load the current state over REST,
 *   2. open the stream,
 *   3. on every (re)connection, re-run the REST load.
 *
 * The browser handles the short gap through `Last-Event-ID`; the REST
 * resynchronisation covers the long one, including an absence longer than the
 * backend's 500-event replay bound. Writing a `fetch`-based SSE parser to
 * control the header would mean reimplementing reconnection, backoff and frame
 * parsing to solve a problem the database already solves (decision D-46).
 *
 * ## Backoff
 *
 * `EventSource` reconnects on its own, but on a fixed browser-chosen interval,
 * which hammers a backend that is down. So the native reconnection is disabled
 * -- by closing the source on error -- and reopening is driven here with an
 * exponential backoff.
 */
@Injectable({ providedIn: 'root' })
export class AlertStreamService implements OnDestroy {
  private readonly config = inject(APP_CONFIG);
  private readonly zone = inject(NgZone);

  private source: EventSource | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private backoffMs = this.config.sseInitialBackoffMs;
  private explicitlyClosed = false;

  private readonly stateSignal = signal<ConnectionState>('disconnected');
  private readonly messages$ = new Subject<StreamMessage>();
  /** Emits whenever a connection is (re)established, so callers can resync. */
  private readonly connected$ = new Subject<void>();

  /** Read-only for components; only this service may change it. */
  readonly state = computed(() => this.stateSignal());
  readonly isLive = computed(() => this.stateSignal() === 'connected');

  readonly events = this.messages$.asObservable();
  readonly connections = this.connected$.asObservable();

  connect(): void {
    if (this.source) {
      return;
    }
    this.explicitlyClosed = false;
    this.open();
  }

  private open(): void {
    this.stateSignal.set(this.backoffMs === this.config.sseInitialBackoffMs ? 'connecting' : 'reconnecting');

    const url = `${this.config.apiBaseUrl}/alerts/stream`;
    const source = new EventSource(url);
    this.source = source;

    source.onopen = () =>
      // EventSource callbacks fire outside Angular, so a signal written here
      // would update without a change-detection pass and the indicator would
      // lag behind reality.
      this.zone.run(() => {
        this.backoffMs = this.config.sseInitialBackoffMs;
        this.stateSignal.set('connected');
        this.connected$.next();
      });

    source.addEventListener('alert.created', (event) =>
      this.zone.run(() => this.emit('alert.created', event as MessageEvent)),
    );
    source.addEventListener('alert.updated', (event) =>
      this.zone.run(() => this.emit('alert.updated', event as MessageEvent)),
    );
    // Heartbeats exist to keep proxies from closing an idle connection. They
    // carry no state and are deliberately not forwarded: a listener that
    // reacted to them would redraw every 15 seconds for nothing.
    source.addEventListener('heartbeat', () => {});

    source.onerror = () =>
      this.zone.run(() => {
        // The browser would retry on its own schedule; closing here puts the
        // timing under our control so a backend outage is not hammered.
        source.close();
        this.source = null;
        if (this.explicitlyClosed) {
          this.stateSignal.set('disconnected');
          return;
        }
        this.stateSignal.set('reconnecting');
        this.scheduleReconnect();
      });
  }

  private emit(name: 'alert.created' | 'alert.updated', event: MessageEvent): void {
    try {
      const parsed = JSON.parse(event.data) as AlertStreamEvent;
      this.messages$.next({ name, event: parsed });
    } catch {
      // A frame we cannot parse must not tear down a working stream; the REST
      // resynchronisation will pick up whatever it described.
      console.warn('[sse] unparseable event payload ignored');
    }
  }

  private scheduleReconnect(): void {
    this.clearTimer();
    const delay = this.backoffMs;
    this.backoffMs = Math.min(this.backoffMs * 2, this.config.sseMaxBackoffMs);
    this.reconnectTimer = setTimeout(() => {
      if (!this.explicitlyClosed) {
        this.open();
      }
    }, delay);
  }

  /** Current retry delay, exposed so a test can prove the backoff grows. */
  get currentBackoffMs(): number {
    return this.backoffMs;
  }

  disconnect(): void {
    this.explicitlyClosed = true;
    this.clearTimer();
    this.source?.close();
    this.source = null;
    this.stateSignal.set('disconnected');
  }

  private clearTimer(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  ngOnDestroy(): void {
    this.disconnect();
    this.messages$.complete();
    this.connected$.complete();
  }
}
