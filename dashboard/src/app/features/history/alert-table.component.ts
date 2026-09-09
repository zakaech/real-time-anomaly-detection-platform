import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';
import { RouterLink } from '@angular/router';
import { AlertSummary } from '../../core/models/alert.model';
import { AlertSortProperty, SortDirection } from '../../core/models/filters.model';
import { SeverityBadgeComponent } from '../../shared/severity-badge.component';
import { StatusBadgeComponent } from '../../shared/status-badge.component';
import { DateTimePipe, ScorePipe } from '../../shared/format.pipes';

/** One header cell. `id` is unique so it can serve as the track key. */
interface Column {
  readonly id: string;
  readonly label: string;
  readonly sortable: boolean;
}

/**
 * The history table.
 *
 * Sorting is emitted, never performed here: the backend whitelists four
 * properties and executes ORDER BY in SQL. Sorting the current page in the
 * browser would order twenty rows out of hundreds and look like a bug.
 */
@Component({
  selector: 'app-alert-table',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, SeverityBadgeComponent, StatusBadgeComponent, DateTimePipe, ScorePipe],
  template: `
    <div class="table-wrapper">
      <table>
        <caption class="visually-hidden">
          Historique des alertes, trié côté serveur
        </caption>
        <thead>
          <tr>
            @for (column of columns; track column.id) {
              <th
                [attr.aria-sort]="ariaSort(column.id)"
                [class.sortable]="column.sortable"
                (click)="onHeaderClick(column)"
              >
                {{ column.label }}
                @if (column.sortable && sort() === column.id) {
                  <span aria-hidden="true">{{ direction() === 'asc' ? '▲' : '▼' }}</span>
                }
              </th>
            }
          </tr>
        </thead>
        <tbody>
          @for (alert of alerts(); track alert.alertId) {
            <tr>
              <td>{{ alert.detectedAt | dateTime }}</td>
              <td><app-severity-badge [severity]="alert.severity" /></td>
              <td class="mono">{{ alert.machineCode }}</td>
              <td>{{ alert.lineCode }}</td>
              <td class="mono">{{ alert.anomalyScore | score }}</td>
              <td><app-status-badge [status]="alert.status" /></td>
              <td><a [routerLink]="['/alerts', alert.alertId]">Détail</a></td>
            </tr>
          }
        </tbody>
      </table>
    </div>
  `,
  styles: [
    `
      .table-wrapper {
        overflow-x: auto;
        background: #fff;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.875rem;
      }
      th,
      td {
        padding: 0.55rem 0.75rem;
        text-align: left;
        white-space: nowrap;
      }
      thead th {
        background: #f8fafc;
        border-bottom: 1px solid #e2e8f0;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: #475569;
      }
      th.sortable {
        cursor: pointer;
        user-select: none;
      }
      th.sortable:hover {
        color: #1d4ed8;
      }
      tbody tr + tr td {
        border-top: 1px solid #f1f5f9;
      }
      tbody tr:hover {
        background: #f8fafc;
      }
      .mono {
        font-family: ui-monospace, monospace;
      }
      .visually-hidden {
        position: absolute;
        width: 1px;
        height: 1px;
        overflow: hidden;
        clip: rect(0 0 0 0);
      }
    `,
  ],
})
export class AlertTableComponent {
  readonly alerts = input.required<readonly AlertSummary[]>();
  readonly sort = input.required<AlertSortProperty>();
  readonly direction = input.required<SortDirection>();
  readonly sortBy = output<AlertSortProperty>();

  /**
   * Column ids are unique because they are the `track` key, and only the four
   * the backend whitelists are sortable -- offering to sort on anything else
   * would produce a 400 from a click.
   */
  protected readonly columns: readonly Column[] = [
    { id: 'detectedAt', label: 'Détectée le', sortable: true },
    { id: 'severity', label: 'Sévérité', sortable: true },
    { id: 'machineCode', label: 'Machine', sortable: false },
    { id: 'lineCode', label: 'Ligne', sortable: false },
    { id: 'anomalyScore', label: 'Score', sortable: true },
    { id: 'status', label: 'Statut', sortable: true },
    { id: 'link', label: '', sortable: false },
  ];

  protected onHeaderClick(column: Column): void {
    if (column.sortable) {
      this.sortBy.emit(column.id as AlertSortProperty);
    }
  }

  protected ariaSort(key: string): 'ascending' | 'descending' | 'none' {
    if (key !== this.sort()) {
      return 'none';
    }
    return this.direction() === 'asc' ? 'ascending' : 'descending';
  }
}
