import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {
  AlertSeverity,
  AlertStatus,
  SEVERITY_ORDER,
  STATUS_ORDER,
} from '../../core/models/alert.model';
import { AlertFilters } from '../../core/models/filters.model';

/**
 * The filter bar.
 *
 * Presentational: it emits the filter object and never queries anything itself.
 * Every value it produces is sent to the API and executed by PostgreSQL --
 * filtering the twenty rows of the current page in the browser would contradict
 * the `totalElements` printed next to them.
 */
@Component({
  selector: 'app-alert-filters',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule],
  template: `
    <form class="filters" (submit)="submit($event)">
      <label>
        <span>Machine</span>
        <input
          name="machineCode"
          [(ngModel)]="machineCode"
          placeholder="M-001"
          pattern="M-[0-9]{3}"
          title="Format attendu : M-001"
        />
      </label>

      <label>
        <span>Ligne</span>
        <input name="lineCode" [(ngModel)]="lineCode" placeholder="LINE-A" pattern="LINE-[A-Z]" />
      </label>

      <fieldset>
        <legend>Sévérité</legend>
        @for (severity of severities; track severity) {
          <label class="check">
            <input
              type="checkbox"
              [checked]="selectedSeverities.has(severity)"
              (change)="toggleSeverity(severity)"
            />
            <span>{{ severity }}</span>
          </label>
        }
      </fieldset>

      <fieldset>
        <legend>Statut</legend>
        @for (status of statuses; track status) {
          <label class="check">
            <input
              type="checkbox"
              [checked]="selectedStatuses.has(status)"
              (change)="toggleStatus(status)"
            />
            <span>{{ status }}</span>
          </label>
        }
      </fieldset>

      <label>
        <span>Du</span>
        <input type="datetime-local" name="from" [(ngModel)]="from" />
      </label>

      <label>
        <span>Au</span>
        <input type="datetime-local" name="to" [(ngModel)]="to" />
      </label>

      <div class="actions">
        <button type="submit" class="primary" [disabled]="disabled()">Filtrer</button>
        <button type="button" (click)="reset()" [disabled]="disabled()">Réinitialiser</button>
      </div>
    </form>
  `,
  styles: [
    `
      .filters {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 0.85rem;
        align-items: end;
        padding: 1rem;
        background: #fff;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        margin-bottom: 1rem;
      }
      label {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        font-size: 0.8rem;
        color: #475569;
      }
      input[type='text'],
      input:not([type]),
      input[type='datetime-local'] {
        padding: 0.4rem 0.5rem;
        border: 1px solid #cbd5e1;
        border-radius: 6px;
        font: inherit;
      }
      fieldset {
        border: 1px solid #e2e8f0;
        border-radius: 6px;
        padding: 0.4rem 0.6rem;
        margin: 0;
      }
      legend {
        font-size: 0.75rem;
        color: #475569;
        padding: 0 0.25rem;
      }
      .check {
        flex-direction: row;
        align-items: center;
        gap: 0.35rem;
        font-size: 0.75rem;
      }
      .actions {
        display: flex;
        gap: 0.5rem;
      }
      button {
        padding: 0.45rem 0.9rem;
        border-radius: 6px;
        border: 1px solid #cbd5e1;
        background: #fff;
        font-weight: 600;
        cursor: pointer;
      }
      button.primary {
        background: #1d4ed8;
        border-color: #1d4ed8;
        color: #fff;
      }
      button:disabled {
        opacity: 0.6;
        cursor: not-allowed;
      }
    `,
  ],
})
export class AlertFiltersComponent {
  readonly disabled = input(false);
  readonly apply = output<AlertFilters>();

  protected readonly severities = SEVERITY_ORDER;
  protected readonly statuses = STATUS_ORDER;

  protected machineCode = '';
  protected lineCode = '';
  protected from = '';
  protected to = '';
  protected readonly selectedSeverities = new Set<AlertSeverity>();
  protected readonly selectedStatuses = new Set<AlertStatus>();

  protected toggleSeverity(severity: AlertSeverity): void {
    if (!this.selectedSeverities.delete(severity)) {
      this.selectedSeverities.add(severity);
    }
  }

  protected toggleStatus(status: AlertStatus): void {
    if (!this.selectedStatuses.delete(status)) {
      this.selectedStatuses.add(status);
    }
  }

  protected submit(event: Event): void {
    event.preventDefault();
    this.apply.emit(this.build());
  }

  protected reset(): void {
    this.machineCode = '';
    this.lineCode = '';
    this.from = '';
    this.to = '';
    this.selectedSeverities.clear();
    this.selectedStatuses.clear();
    this.apply.emit({});
  }

  /**
   * Only the fields the operator actually filled are emitted.
   *
   * Sending empty strings would make the backend filter on `machineCode = ''`
   * and return nothing, which looks like "no data" rather than a bad request.
   */
  private build(): AlertFilters {
    return {
      machineCode: this.machineCode.trim() || undefined,
      lineCode: this.lineCode.trim() || undefined,
      severity: this.selectedSeverities.size ? [...this.selectedSeverities] : undefined,
      status: this.selectedStatuses.size ? [...this.selectedStatuses] : undefined,
      from: toInstant(this.from),
      to: toInstant(this.to),
    };
  }
}

/**
 * `datetime-local` yields a local wall-clock string with no zone. The API takes
 * instants, so it is converted here rather than sent as-is -- passing the raw
 * value would silently shift the range by the browser's offset.
 */
function toInstant(localValue: string): string | undefined {
  if (!localValue) {
    return undefined;
  }
  const date = new Date(localValue);
  return Number.isNaN(date.getTime()) ? undefined : date.toISOString();
}
