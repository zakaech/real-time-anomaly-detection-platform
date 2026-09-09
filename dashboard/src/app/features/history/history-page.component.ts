import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed, toObservable } from '@angular/core/rxjs-interop';
import { debounceTime, distinctUntilChanged, switchMap } from 'rxjs';
import { AlertApiService } from '../../core/api/alert-api.service';
import { AlertSummary } from '../../core/models/alert.model';
import {
  AlertFilters,
  AlertQuery,
  AlertSortProperty,
  DEFAULT_QUERY,
} from '../../core/models/filters.model';
import { ApiError } from '../../core/models/problem-detail.model';
import {
  EmptyStateComponent,
  ErrorStateComponent,
  LoadingStateComponent,
} from '../../shared/state-views.component';
import { AlertFiltersComponent } from './alert-filters.component';
import { AlertTableComponent } from './alert-table.component';
import { PaginatorComponent } from './paginator.component';

/**
 * The history page.
 *
 * The query is a signal; every change to it becomes one request through
 * `switchMap`, which cancels the in-flight one. That matters for correctness and
 * not just for load: without it, a slow response to an old filter can arrive
 * after a fast response to a new one and overwrite the table with stale rows.
 */
@Component({
  selector: 'app-history-page',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    AlertFiltersComponent,
    AlertTableComponent,
    PaginatorComponent,
    LoadingStateComponent,
    EmptyStateComponent,
    ErrorStateComponent,
  ],
  template: `
    <section class="page">
      <header class="page-header">
        <h2>Historique</h2>
        <p class="subtitle">
          Filtres, tri et pagination exécutés par PostgreSQL&nbsp;: le navigateur ne reçoit qu'une
          page.
        </p>
      </header>

      <app-alert-filters [disabled]="loading()" (apply)="applyFilters($event)" />

      @if (error(); as err) {
        <app-error-state
          title="Impossible de charger l'historique"
          [message]="err.message"
          [traceId]="err.problem?.traceId ?? null"
          (retry)="reload()"
        />
      } @else if (loading() && alerts().length === 0) {
        <app-loading-state message="Chargement de l'historique..." />
      } @else if (alerts().length === 0) {
        <app-empty-state
          title="Aucune alerte ne correspond"
          hint="Élargissez la plage de dates ou réinitialisez les filtres."
        />
      } @else {
        <app-alert-table
          [alerts]="alerts()"
          [sort]="query().sort"
          [direction]="query().direction"
          (sortBy)="toggleSort($event)"
        />
        <app-paginator
          [page]="query().page"
          [size]="query().size"
          [totalElements]="totalElements()"
          [totalPages]="totalPages()"
          [last]="last()"
          [busy]="loading()"
          (goTo)="goToPage($event)"
        />
      }
    </section>
  `,
  styles: [
    `
      h2 {
        margin: 0;
        font-size: 1.2rem;
      }
      .subtitle {
        margin: 0.2rem 0 1rem;
        font-size: 0.85rem;
        color: #64748b;
      }
    `,
  ],
})
export class HistoryPageComponent {
  private readonly api = inject(AlertApiService);

  protected readonly query = signal<AlertQuery>(DEFAULT_QUERY);
  protected readonly alerts = signal<readonly AlertSummary[]>([]);
  protected readonly totalElements = signal(0);
  protected readonly totalPages = signal(0);
  protected readonly last = signal(true);
  protected readonly loading = signal(false);
  protected readonly error = signal<ApiError | null>(null);

  constructor() {
    toObservable(this.query)
      .pipe(
        // A rapid sequence of filter changes issues one request, not five.
        debounceTime(250),
        distinctUntilChanged((a, b) => JSON.stringify(a) === JSON.stringify(b)),
        switchMap((query) => {
          this.loading.set(true);
          this.error.set(null);
          return this.api.listAlerts(query);
        }),
        takeUntilDestroyed(),
      )
      .subscribe({
        next: (page) => {
          this.alerts.set(page.content);
          this.totalElements.set(page.totalElements);
          this.totalPages.set(page.totalPages);
          this.last.set(page.last);
          this.loading.set(false);
        },
        error: (err: ApiError) => {
          this.error.set(err);
          this.loading.set(false);
        },
      });
  }

  protected applyFilters(filters: AlertFilters): void {
    // Back to the first page: keeping page 7 while narrowing the filter is how
    // a user lands on an empty page and concludes there is no data.
    this.query.update((current) => ({ ...current, ...filters, page: 0 }));
  }

  protected toggleSort(property: AlertSortProperty): void {
    this.query.update((current) => ({
      ...current,
      sort: property,
      direction: current.sort === property && current.direction === 'desc' ? 'asc' : 'desc',
      page: 0,
    }));
  }

  protected goToPage(page: number): void {
    this.query.update((current) => ({ ...current, page: Math.max(0, page) }));
  }

  protected reload(): void {
    // A new object identity, so distinctUntilChanged does not swallow the retry.
    this.query.update((current) => ({ ...current }));
    this.error.set(null);
    this.api.listAlerts(this.query()).subscribe({
      next: (page) => {
        this.alerts.set(page.content);
        this.totalElements.set(page.totalElements);
        this.totalPages.set(page.totalPages);
        this.last.set(page.last);
      },
      error: (err: ApiError) => this.error.set(err),
    });
  }
}
