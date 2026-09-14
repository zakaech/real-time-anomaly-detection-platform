import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { ChartConfiguration } from 'chart.js';
import { Contributor } from '../../core/models/alert.model';
import { TimeSeriesChartComponent } from '../../shared/time-series-chart.component';

/**
 * The features that departed most from the training reference.
 *
 * The only per-feature detail an alert carries: the full 52-feature vector is
 * never parsed by the alerting query, so it is not available here.
 *
 * Note `z_score`, not `zScore` -- the backend reuses one DTO for the Kafka
 * message and the REST response, and the Kafka side is snake_case (D-45).
 */
@Component({
  selector: 'app-contributor-chart',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [TimeSeriesChartComponent],
  template: `
    @if (contributors().length === 0) {
      <p class="none">Aucun contributeur enregistré pour cette alerte.</p>
    } @else {
      <app-time-series-chart
        [configuration]="config()"
        [height]="height()"
        caption="Écart à la référence d'entraînement (z-score)"
        [ariaLabel]="ariaText()"
      />
    }
  `,
  styles: [
    `
      .none {
        margin: 0;
        font-size: 0.85rem;
        color: #64748b;
      }
    `,
  ],
})
export class ContributorChartComponent {
  readonly contributors = input.required<readonly Contributor[]>();

  protected readonly height = computed(() => Math.max(120, this.contributors().length * 46));

  protected readonly ariaText = computed(() =>
    this.contributors()
      .map((c) => `${c.feature} : z-score ${c.z_score.toFixed(2)}`)
      .join(', '),
  );

  protected readonly config = computed<ChartConfiguration>(() => {
    const items = [...this.contributors()].sort(
      (a, b) => Math.abs(b.z_score) - Math.abs(a.z_score),
    );
    return {
      type: 'bar',
      data: {
        labels: items.map((item) => item.feature),
        datasets: [
          {
            label: 'z-score',
            data: items.map((item) => item.z_score),
            // A negative departure is as meaningful as a positive one, so the
            // sign is shown by colour rather than hidden by an absolute value.
            backgroundColor: items.map((item) => (item.z_score >= 0 ? '#dc2626' : '#2563eb')),
            borderWidth: 0,
          },
        ],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { title: { display: true, text: 'z-score' }, grid: { color: '#f1f5f9' } },
          y: { grid: { display: false } },
        },
        plugins: { legend: { display: false } },
      },
    };
  });
}
