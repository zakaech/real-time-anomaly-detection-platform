import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { AlertSeverity } from '../../core/models/alert.model';

/**
 * Live counters.
 *
 * Presentational: it receives already-computed numbers. The counts are derived
 * in the store from the alert map rather than incremented on arrival, because a
 * hand-maintained counter drifts on the first duplicate and never recovers.
 */
@Component({
  selector: 'app-severity-counters',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="counters">
      <div class="counter total">
        <span class="value">{{ total() }}</span>
        <span class="label">Alertes affichées</span>
      </div>
      <div class="counter new">
        <span class="value">{{ newCount() }}</span>
        <span class="label">Non acquittées</span>
      </div>
      <div class="counter critical">
        <span class="value">{{ counts().CRITICAL }}</span>
        <span class="label">Critiques</span>
      </div>
      <div class="counter high">
        <span class="value">{{ counts().HIGH }}</span>
        <span class="label">Élevées</span>
      </div>
      <div class="counter medium">
        <span class="value">{{ counts().MEDIUM }}</span>
        <span class="label">Moyennes</span>
      </div>
    </div>
  `,
  styles: [
    `
      .counters {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 0.75rem;
        margin-bottom: 1.25rem;
      }
      .counter {
        background: #fff;
        border: 1px solid #e2e8f0;
        border-left-width: 4px;
        border-radius: 8px;
        padding: 0.85rem 1rem;
        display: flex;
        flex-direction: column;
        gap: 0.15rem;
      }
      .value {
        font-size: 1.6rem;
        font-weight: 700;
        line-height: 1;
      }
      .label {
        font-size: 0.75rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
      .total {
        border-left-color: #0f172a;
      }
      .new {
        border-left-color: #2563eb;
      }
      .critical {
        border-left-color: #dc2626;
      }
      .high {
        border-left-color: #ea580c;
      }
      .medium {
        border-left-color: #ca8a04;
      }
    `,
  ],
})
export class SeverityCountersComponent {
  readonly total = input.required<number>();
  readonly newCount = input.required<number>();
  readonly counts = input.required<Record<AlertSeverity, number>>();
}
