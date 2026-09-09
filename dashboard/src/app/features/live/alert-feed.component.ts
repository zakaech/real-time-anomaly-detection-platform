import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { RouterLink } from '@angular/router';
import { AlertSummary } from '../../core/models/alert.model';
import { SeverityBadgeComponent } from '../../shared/severity-badge.component';
import { StatusBadgeComponent } from '../../shared/status-badge.component';
import { ScorePipe, ShortTimePipe } from '../../shared/format.pipes';

/**
 * The incoming alert feed.
 *
 * Presentational: a list in, rows out. `track` is keyed on the alert id, which
 * is what lets an acknowledgement update a row in place instead of Angular
 * destroying and recreating it -- the visual counterpart of the deduplication
 * the store performs.
 */
@Component({
  selector: 'app-alert-feed',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, SeverityBadgeComponent, StatusBadgeComponent, ScorePipe, ShortTimePipe],
  template: `
    <ul class="feed">
      @for (alert of alerts(); track alert.alertId) {
        <li class="row" [class.acknowledged]="alert.status !== 'NEW'">
          <a [routerLink]="['/alerts', alert.alertId]" class="link">
            <span class="time">{{ alert.detectedAt | shortTime }}</span>
            <app-severity-badge [severity]="alert.severity" />
            <span class="machine">{{ alert.machineCode }}</span>
            <span class="line">{{ alert.lineCode }}</span>
            <span class="score" title="Score d'anomalie">{{ alert.anomalyScore | score }}</span>
            <app-status-badge [status]="alert.status" />
          </a>
        </li>
      }
    </ul>
  `,
  styles: [
    `
      .feed {
        list-style: none;
        margin: 0;
        padding: 0;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        overflow: hidden;
        background: #fff;
      }
      .row + .row {
        border-top: 1px solid #f1f5f9;
      }
      .row.acknowledged {
        background: #fafafa;
      }
      .link {
        display: grid;
        grid-template-columns: 6rem 6rem 5rem 5rem 1fr 7rem;
        gap: 0.75rem;
        align-items: center;
        padding: 0.6rem 0.9rem;
        text-decoration: none;
        color: inherit;
        font-size: 0.875rem;
      }
      .link:hover {
        background: #f8fafc;
      }
      .link:focus-visible {
        outline: 2px solid #2563eb;
        outline-offset: -2px;
      }
      .time {
        font-variant-numeric: tabular-nums;
        color: #475569;
      }
      .machine {
        font-weight: 600;
      }
      .line {
        color: #64748b;
      }
      .score {
        font-family: ui-monospace, monospace;
        font-size: 0.8rem;
        color: #334155;
      }
      @media (max-width: 900px) {
        .link {
          grid-template-columns: 5rem 5.5rem 1fr;
          row-gap: 0.35rem;
        }
        .line,
        .score {
          display: none;
        }
      }
    `,
  ],
})
export class AlertFeedComponent {
  readonly alerts = input.required<readonly AlertSummary[]>();
}
