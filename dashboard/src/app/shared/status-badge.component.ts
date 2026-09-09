import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { AlertStatus } from '../core/models/alert.model';

/** Alert status as an operator sees it. Presentational only. */
@Component({
  selector: 'app-status-badge',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `<span class="badge" [class]="cssClass()">{{ text() }}</span>`,
  styles: [
    `
      .badge {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 600;
        border: 1px solid transparent;
      }
      .new {
        background: #dbeafe;
        color: #1e40af;
        border-color: #93c5fd;
      }
      .acknowledged {
        background: #dcfce7;
        color: #166534;
        border-color: #86efac;
      }
      .resolved,
      .dismissed {
        background: #f1f5f9;
        color: #475569;
        border-color: #cbd5e1;
      }
    `,
  ],
})
export class StatusBadgeComponent {
  readonly status = input.required<AlertStatus>();

  private static readonly LABELS: Record<AlertStatus, string> = {
    NEW: 'Nouvelle',
    ACKNOWLEDGED: 'Acquittée',
    RESOLVED: 'Résolue',
    DISMISSED: 'Écartée',
  };

  protected readonly cssClass = computed(() => this.status().toLowerCase());
  protected readonly text = computed(() => StatusBadgeComponent.LABELS[this.status()]);
}
