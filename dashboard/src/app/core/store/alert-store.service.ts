import { DestroyRef, Injectable, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { AlertApiService } from '../api/alert-api.service';
import { APP_CONFIG } from '../config/app-config';
import { AlertSeverity, AlertSummary } from '../models/alert.model';
import { DEFAULT_QUERY } from '../models/filters.model';
import { AlertStreamEvent } from '../models/stream-event.model';
import { AlertStreamService } from '../sse/alert-stream.service';

/**
 * What the live page displays.
 *
 * ## This is a projection, not a second source of truth
 *
 * PostgreSQL holds the alerts. This holds what is currently on screen, and it is
 * rebuilt from REST whenever the stream reconnects. Keeping a parallel state
 * that could disagree with the database is exactly what the backend design
 * warns against, so the store never invents an entry the API has not confirmed.
 *
 * ## Deduplication
 *
 * Alerts are keyed by `alertId` in a Map and always upserted, never pushed.
 * That single choice covers three different causes of a repeat:
 *
 * - the same SSE event delivered twice;
 * - a reconnection replaying up to 500 events through `Last-Event-ID`;
 * - the REST resynchronisation overlapping with events already received.
 *
 * The counters are `computed` from the Map rather than incremented on arrival:
 * a hand-maintained counter would drift on the first duplicate and never
 * recover.
 */
@Injectable({ providedIn: 'root' })
export class AlertStoreService {
  private readonly api = inject(AlertApiService);
  private readonly stream = inject(AlertStreamService);
  private readonly config = inject(APP_CONFIG);
  private readonly destroyRef = inject(DestroyRef);

  private readonly alerts = signal<ReadonlyMap<string, AlertSummary>>(new Map());
  private readonly loadingSignal = signal(false);
  private readonly errorSignal = signal<string | null>(null);
  /** Highest event sequence seen, so a late duplicate can be recognised. */
  private readonly lastSeq = signal(0);

  readonly loading = computed(() => this.loadingSignal());
  readonly error = computed(() => this.errorSignal());
  readonly connectionState = this.stream.state;
  readonly lastEventSeq = computed(() => this.lastSeq());

  /** Newest first, bounded so an unbounded stream cannot grow the DOM forever. */
  readonly feed = computed(() =>
    [...this.alerts().values()]
      .sort((a, b) => b.detectedAt.localeCompare(a.detectedAt))
      .slice(0, this.config.liveFeedLimit),
  );

  readonly total = computed(() => this.alerts().size);

  readonly countsBySeverity = computed(() => {
    const counts: Record<AlertSeverity, number> = { MEDIUM: 0, HIGH: 0, CRITICAL: 0 };
    for (const alert of this.alerts().values()) {
      counts[alert.severity]++;
    }
    return counts;
  });

  readonly newCount = computed(
    () => [...this.alerts().values()].filter((a) => a.status === 'NEW').length,
  );

  /**
   * Start the live view: load the current state, then follow the stream.
   *
   * The order matters. Opening the stream first would leave a window in which
   * events arrive with nothing to merge them into.
   */
  start(): void {
    this.refresh();

    this.stream.events.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(({ event }) => {
      // An update and a creation are handled identically: both are an upsert
      // keyed on the alert id, so an acknowledgement replaces the row instead of
      // adding a second one.
      this.upsert(event);
    });

    // Every reconnection triggers a full REST reload. Last-Event-ID covers a
    // short gap; this covers a long one, including an absence longer than the
    // backend's replay bound.
    this.stream.connections.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
      this.refresh();
    });

    this.stream.connect();
  }

  stop(): void {
    this.stream.disconnect();
  }

  /** Reload the visible window from the API, replacing whatever is held. */
  refresh(): void {
    this.loadingSignal.set(true);
    this.errorSignal.set(null);

    this.api
      .listAlerts({ ...DEFAULT_QUERY, size: this.config.liveFeedLimit })
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (page) => {
          // Replace rather than merge: the API is authoritative, and merging
          // would keep an alert the database no longer returns.
          const next = new Map<string, AlertSummary>();
          for (const alert of page.content) {
            next.set(alert.alertId, alert);
          }
          this.alerts.set(next);
          this.loadingSignal.set(false);
        },
        error: (error: Error) => {
          this.errorSignal.set(error.message);
          this.loadingSignal.set(false);
        },
      });
  }

  /**
   * Merge one streamed event.
   *
   * The event carries every field `AlertSummary` needs except the two the list
   * endpoint adds, so the existing row is used as the base when there is one
   * and the missing fields are left at what the event can prove.
   */
  private upsert(event: AlertStreamEvent): void {
    this.lastSeq.update((seq) => Math.max(seq, event.eventSeq));

    this.alerts.update((current) => {
      const existing = current.get(event.alertId);
      const merged: AlertSummary = {
        alertId: event.alertId,
        machineCode: event.machineCode,
        lineCode: event.lineCode,
        severity: event.severity,
        status: event.status,
        anomalyScore: event.anomalyScore,
        detectedAt: event.detectedAt,
        // Not carried by the stream event. Keeping what the REST load gave is
        // better than defaulting to a number the backend never sent.
        scoreThreshold: existing?.scoreThreshold ?? 0,
        consecutiveWindows: existing?.consecutiveWindows ?? 0,
      };

      const next = new Map(current);
      next.set(event.alertId, merged);
      return next;
    });
  }
}
