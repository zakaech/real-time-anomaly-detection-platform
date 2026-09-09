import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  input,
  signal,
} from '@angular/core';
import { AlertApiService } from '../../core/api/alert-api.service';
import { AlertDetail } from '../../core/models/alert.model';
import { ApiError } from '../../core/models/problem-detail.model';
import { TelemetrySeries } from '../../core/models/telemetry.model';
import { SeverityBadgeComponent } from '../../shared/severity-badge.component';
import { StatusBadgeComponent } from '../../shared/status-badge.component';
import { DateTimePipe, DurationSecondsPipe, ScorePipe } from '../../shared/format.pipes';
import {
  ErrorStateComponent,
  LoadingStateComponent,
} from '../../shared/state-views.component';
import {
  AcknowledgeFormComponent,
  AcknowledgeSubmission,
} from './acknowledge-form.component';
import { ContributorChartComponent } from './contributor-chart.component';
import { TelemetryChartComponent } from './telemetry-chart.component';

/** Minutes of context shown either side of the alert window. */
const CONTEXT_MINUTES = 15;

/**
 * Everything the platform knows about one alert.
 *
 * The telemetry curve is loaded around the alert window, so an operator sees
 * what the machine was doing before and after it fired. What is NOT shown is
 * equally deliberate: there is no raw sensor trace, because raw telemetry is
 * never persisted. The curve is the per-window mean the pipeline computed, and
 * the caption says so.
 */
@Component({
  selector: 'app-alert-detail-page',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    SeverityBadgeComponent,
    StatusBadgeComponent,
    DateTimePipe,
    DurationSecondsPipe,
    ScorePipe,
    LoadingStateComponent,
    ErrorStateComponent,
    AcknowledgeFormComponent,
    ContributorChartComponent,
    TelemetryChartComponent,
  ],
  templateUrl: './alert-detail-page.component.html',
  styleUrl: './alert-detail-page.component.scss',
})
export class AlertDetailPageComponent {
  private readonly api = inject(AlertApiService);

  /** Bound from the route by `withComponentInputBinding`. */
  readonly alertId = input.required<string>();

  protected readonly alert = signal<AlertDetail | null>(null);
  protected readonly telemetry = signal<TelemetrySeries | null>(null);
  protected readonly loading = signal(true);
  protected readonly error = signal<ApiError | null>(null);
  protected readonly acknowledging = signal(false);
  protected readonly conflictMessage = signal<string | null>(null);
  protected readonly acknowledged = signal(false);

  protected readonly canAcknowledge = computed(() => this.alert()?.status === 'NEW');

  constructor() {
    // The constructor is an injection context, so the effect can be created
    // here directly. It reacts to the route parameter, which means navigating
    // from one alert to another reloads instead of leaving the previous one on
    // screen.
    effect(() => {
      const id = this.alertId();
      if (id) {
        this.load(id);
      }
    });
  }

  protected load(alertId: string): void {
    this.loading.set(true);
    this.error.set(null);
    this.api.getAlert(alertId).subscribe({
      next: (detail) => {
        this.alert.set(detail);
        this.loading.set(false);
        this.loadTelemetry(detail);
      },
      error: (err: ApiError) => {
        this.error.set(err);
        this.loading.set(false);
      },
    });
  }

  /**
   * The curve around the alert.
   *
   * A telemetry failure does not fail the page: the alert itself is the
   * operational information, and a missing chart is a degraded view rather than
   * a broken one.
   */
  private loadTelemetry(detail: AlertDetail): void {
    const from = new Date(
      new Date(detail.windowStart).getTime() - CONTEXT_MINUTES * 60_000,
    ).toISOString();
    const to = new Date(
      new Date(detail.windowEnd).getTime() + CONTEXT_MINUTES * 60_000,
    ).toISOString();

    this.api.getTelemetry(detail.machineCode, from, to, 1000).subscribe({
      next: (series) => this.telemetry.set(series),
      error: () => this.telemetry.set(null),
    });
  }

  protected onAcknowledge(submission: AcknowledgeSubmission): void {
    const current = this.alert();
    if (!current) {
      return;
    }
    this.acknowledging.set(true);
    this.conflictMessage.set(null);

    this.api
      .acknowledge(
        current.alertId,
        {
          comment: submission.comment || undefined,
          // Optional concurrency check (D-09): sending the version we displayed
          // turns a concurrent change into a 409 instead of a silent overwrite.
          expectedVersion: current.version,
        },
        submission.operator,
      )
      .subscribe({
        next: (updated) => {
          this.alert.set(updated);
          this.acknowledging.set(false);
          this.acknowledged.set(true);
        },
        error: (err: ApiError) => {
          this.acknowledging.set(false);
          if (err.isConflict) {
            // The 409 carries the current state, so the operator is told what
            // it actually is instead of a generic failure, and the page is
            // reloaded to match.
            const status = err.problem?.currentStatus ?? 'inconnu';
            this.conflictMessage.set(
              `Cette alerte a changé entre-temps (statut actuel : ${status}). Les données ont été rechargées.`,
            );
            this.load(current.alertId);
          } else {
            this.error.set(err);
          }
        },
      });
  }
}
