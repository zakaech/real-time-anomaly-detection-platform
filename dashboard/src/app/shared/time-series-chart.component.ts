import {
  AfterViewInit,
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  OnDestroy,
  input,
  effect,
  viewChild,
} from '@angular/core';
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart,
  ChartConfiguration,
  Filler,
  Legend,
  LinearScale,
  LineController,
  LineElement,
  PointElement,
  ScatterController,
  TimeScale,
  Title,
  Tooltip,
} from 'chart.js';
import 'chartjs-adapter-date-fns';

/**
 * The only component that knows Chart.js exists.
 *
 * Chart.js was chosen over ECharts and the Angular wrappers because it is the
 * only candidate with no Angular peer dependency at all (verified: its
 * `peerDependencies` is empty), so it cannot block an Angular upgrade. Wrapping
 * it here rather than adding ng2-charts avoids a second dependency that tracks
 * Angular major versions.
 *
 * Only the pieces actually used are registered, so the bundle carries the line,
 * scatter and bar controllers and nothing else. Chart.js 4 is tree-shakable,
 * and a chart type left out of this list does not degrade: it throws
 * `"<type>" is not a registered controller` at construction and leaves an
 * empty canvas.
 */
Chart.register(
  LineController,
  ScatterController,
  BarController,
  LineElement,
  PointElement,
  BarElement,
  LinearScale,
  CategoryScale,
  TimeScale,
  Filler,
  Legend,
  Title,
  Tooltip,
);

@Component({
  selector: 'app-time-series-chart',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <figure>
      @if (caption()) {
        <figcaption>{{ caption() }}</figcaption>
      }
      <div class="canvas-host" [style.height.px]="height()">
        <canvas #canvas [attr.aria-label]="ariaLabel()" role="img"></canvas>
      </div>
    </figure>
  `,
  styles: [
    `
      figure {
        margin: 0;
      }
      figcaption {
        font-size: 0.8rem;
        font-weight: 600;
        color: #334155;
        margin-bottom: 0.4rem;
      }
      .canvas-host {
        position: relative;
        width: 100%;
      }
    `,
  ],
})
export class TimeSeriesChartComponent implements AfterViewInit, OnDestroy {
  readonly configuration = input.required<ChartConfiguration>();
  readonly height = input(260);
  readonly caption = input<string | null>(null);
  /** A chart is an image to a screen reader; it needs a description. */
  readonly ariaLabel = input('Graphique');

  private readonly canvas = viewChild.required<ElementRef<HTMLCanvasElement>>('canvas');
  private chart: Chart | null = null;
  private ready = false;

  constructor() {
    effect(() => {
      const config = this.configuration();
      if (this.ready) {
        this.render(config);
      }
    });
  }

  ngAfterViewInit(): void {
    this.ready = true;
    this.render(this.configuration());
  }

  private render(config: ChartConfiguration): void {
    // Destroy and recreate rather than mutate: Chart.js keeps internal state
    // keyed to the dataset shape, and updating in place after the series
    // structure changed is a known source of stale axes.
    this.chart?.destroy();
    this.chart = new Chart(this.canvas().nativeElement, config);
  }

  ngOnDestroy(): void {
    // Chart.js attaches resize listeners to the window; not destroying leaks
    // them on every navigation.
    this.chart?.destroy();
    this.chart = null;
  }
}
