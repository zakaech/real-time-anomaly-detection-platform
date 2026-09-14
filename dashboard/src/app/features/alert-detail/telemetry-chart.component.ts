import { ChangeDetectionStrategy, Component, computed, input, signal } from '@angular/core';
import { ChartConfiguration } from 'chart.js';
import {
  SENSOR_SIGNALS,
  SensorKey,
  TelemetryPoint,
  TelemetrySeries,
} from '../../core/models/telemetry.model';
import { TimeSeriesChartComponent } from '../../shared/time-series-chart.component';

/**
 * The sensor curve of one machine, with the detected anomalies marked.
 *
 * Every point comes from a window the pipeline scored and published (D-41).
 * Two rules govern what is drawn:
 *
 * - **a null reading breaks the line.** Chart.js is told `spanGaps: false`, so a
 *   sensor that produced nothing leaves a visible hole instead of a straight
 *   line between the two surrounding points. Drawing through the gap would
 *   invent a measurement.
 * - **anomalies are marked, not inferred.** The red points are the windows the
 *   model flagged, taken from `anomaly` on the row; the chart does not
 *   re-derive them from the score.
 */
@Component({
  selector: 'app-telemetry-chart',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TimeSeriesChartComponent],
  template: `
    @if (series(); as data) {
      @if (data.points.length === 0) {
        <p class="no-data">
          Aucune fenêtre de télémétrie conservée pour cette période. Les données brutes ne sont pas
          persistées&nbsp;; seules les fenêtres scorées le sont.
        </p>
      } @else {
        <div class="signal-picker" role="group" aria-label="Signal affiché">
          @for (signal of signals; track signal.key) {
            <button
              type="button"
              [class.active]="selected() === signal.key"
              (click)="select(signal.key)"
            >
              {{ signal.label }}
            </button>
          }
        </div>

        <app-time-series-chart
          [configuration]="chartConfig()"
          [caption]="captionText()"
          [ariaLabel]="ariaText()"
          [height]="300"
        />

        @if (data.truncated) {
          <p class="notice">
            Série tronquée&nbsp;: la plage contient plus de fenêtres que la limite demandée.
          </p>
        }
      }
    }
  `,
  styles: [
    `
      .signal-picker {
        display: flex;
        flex-wrap: wrap;
        gap: 0.35rem;
        margin-bottom: 0.6rem;
      }
      .signal-picker button {
        padding: 0.3rem 0.7rem;
        border: 1px solid #cbd5e1;
        background: #fff;
        border-radius: 999px;
        font-size: 0.8rem;
        cursor: pointer;
      }
      .signal-picker button.active {
        background: #1d4ed8;
        border-color: #1d4ed8;
        color: #fff;
        font-weight: 600;
      }
      .no-data,
      .notice {
        margin: 0.5rem 0 0;
        font-size: 0.8rem;
        color: #64748b;
      }
      .notice {
        color: #b45309;
      }
    `,
  ],
})
export class TelemetryChartComponent {
  readonly series = input.required<TelemetrySeries | null>();
  /** Highlighted band: the window the alert was raised on. */
  readonly windowStart = input<string | null>(null);
  readonly windowEnd = input<string | null>(null);

  protected readonly signals = SENSOR_SIGNALS;

  /** Which signal is on screen. Temperature first: it is the one an operator
   * looks at first. */
  private readonly selectedSignal = signal<SensorKey>('temperatureCMean');
  protected readonly selected = this.selectedSignal.asReadonly();

  protected select(key: SensorKey): void {
    this.selectedSignal.set(key);
  }

  private readonly signalMeta = computed(
    () => SENSOR_SIGNALS.find((s) => s.key === this.selected()) ?? SENSOR_SIGNALS[0],
  );

  protected readonly captionText = computed(() => {
    const meta = this.signalMeta();
    const data = this.series();
    const seconds = data?.windowSeconds ?? 0;
    return `${meta.label} (${meta.unit}) — moyenne par fenêtre de ${seconds} s`;
  });

  protected readonly ariaText = computed(() => {
    const data = this.series();
    const anomalies = (data?.points ?? []).filter((p) => p.anomaly === true).length;
    return `Courbe ${this.signalMeta().label} sur ${data?.points.length ?? 0} fenêtres, ${anomalies} anomalies détectées`;
  });

  protected readonly chartConfig = computed<ChartConfiguration>(() => {
    const data = this.series();
    const meta = this.signalMeta();
    const points = [...(data?.points ?? [])];
    const key = this.selected();

    const line = points.map((point) => ({
      x: new Date(point.windowStart).getTime(),
      // null keeps the gap visible instead of drawing through a missing reading.
      y: readSensor(point, key),
    }));

    const anomalies = points
      .filter((point) => point.anomaly === true)
      .map((point) => ({
        x: new Date(point.windowStart).getTime(),
        y: readSensor(point, key),
      }))
      .filter((entry) => entry.y !== null);

    return {
      type: 'line',
      data: {
        datasets: [
          {
            label: `${meta.label} (${meta.unit})`,
            data: line,
            borderColor: meta.colour,
            backgroundColor: 'transparent',
            borderWidth: 1.6,
            pointRadius: 0,
            tension: 0,
            // A hole in the data stays a hole.
            spanGaps: false,
          },
          {
            type: 'scatter',
            label: 'Anomalie détectée',
            data: anomalies,
            borderColor: '#b91c1c',
            backgroundColor: '#ef4444',
            pointRadius: 5,
            pointStyle: 'triangle',
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'nearest', intersect: false },
        scales: {
          x: {
            type: 'time',
            time: { tooltipFormat: 'PPpp' },
            ticks: { maxRotation: 0, autoSkip: true },
            grid: { display: false },
          },
          y: {
            title: { display: true, text: meta.unit },
            grid: { color: '#f1f5f9' },
          },
        },
        plugins: {
          legend: { display: true, position: 'bottom', labels: { boxWidth: 12 } },
          tooltip: {
            callbacks: {
              label: (item) =>
                `${item.dataset.label}: ${item.parsed.y === null ? 'aucune mesure' : item.parsed.y}`,
            },
          },
        },
      },
    };
  });
}

/** Reads one sensor mean, preserving null so the gap survives to the chart. */
function readSensor(point: TelemetryPoint, key: SensorKey): number | null {
  const value = point[key];
  return typeof value === 'number' ? value : null;
}
