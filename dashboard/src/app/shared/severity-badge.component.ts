import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { AlertSeverity } from '../core/models/alert.model';

/**
 * Severity, colour-coded.
 *
 * Purely presentational: it takes a value and renders it, injects nothing, and
 * can be rendered in a test without any module setup. The colour mapping lives
 * in TypeScript rather than in a template expression, so the template contains
 * no logic to get wrong.
 */
@Component({
  selector: 'app-severity-badge',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `<span class="badge" [class]="cssClass()" [attr.aria-label]="label()">{{
    severity()
  }}</span>`,
  styles: [
    `
      .badge {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.03em;
        border: 1px solid transparent;
      }
      .medium {
        background: #fef9c3;
        color: #854d0e;
        border-color: #fde047;
      }
      .high {
        background: #ffedd5;
        color: #9a3412;
        border-color: #fb923c;
      }
      .critical {
        background: #fee2e2;
        color: #991b1b;
        border-color: #ef4444;
      }
    `,
  ],
})
export class SeverityBadgeComponent {
  readonly severity = input.required<AlertSeverity>();

  protected readonly cssClass = computed(() => this.severity().toLowerCase());
  protected readonly label = computed(() => `Sévérité ${this.severity()}`);
}
