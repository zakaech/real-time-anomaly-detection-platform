import { ChangeDetectionStrategy, Component, computed, input, output } from '@angular/core';

/**
 * Page navigation.
 *
 * It reports the totals the server returned rather than counting rows locally:
 * the browser only ever holds one page, so any count it computed would be the
 * page size, not the result size.
 */
@Component({
  selector: 'app-paginator',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <nav class="paginator" aria-label="Pagination">
      <span class="summary"> {{ rangeLabel() }} sur {{ totalElements() }} </span>
      <div class="controls">
        <button type="button" [disabled]="page() === 0 || busy()" (click)="goTo.emit(0)">
          « Première
        </button>
        <button type="button" [disabled]="page() === 0 || busy()" (click)="goTo.emit(page() - 1)">
          ‹ Précédente
        </button>
        <span class="position">Page {{ page() + 1 }} / {{ displayTotalPages() }}</span>
        <button type="button" [disabled]="last() || busy()" (click)="goTo.emit(page() + 1)">
          Suivante ›
        </button>
      </div>
    </nav>
  `,
  styles: [
    `
      .paginator {
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
        align-items: center;
        justify-content: space-between;
        padding: 0.75rem 0.25rem;
        font-size: 0.85rem;
        color: #475569;
      }
      .controls {
        display: flex;
        gap: 0.35rem;
        align-items: center;
      }
      button {
        padding: 0.35rem 0.7rem;
        border: 1px solid #cbd5e1;
        background: #fff;
        border-radius: 6px;
        cursor: pointer;
        font: inherit;
      }
      button:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }
      .position {
        padding: 0 0.5rem;
        font-variant-numeric: tabular-nums;
      }
    `,
  ],
})
export class PaginatorComponent {
  readonly page = input.required<number>();
  readonly size = input.required<number>();
  readonly totalElements = input.required<number>();
  readonly totalPages = input.required<number>();
  readonly last = input.required<boolean>();
  readonly busy = input(false);
  readonly goTo = output<number>();

  /** An empty result still reads as "page 1 of 1" rather than "1 of 0". */
  protected readonly displayTotalPages = computed(() => Math.max(this.totalPages(), 1));

  protected readonly rangeLabel = computed(() => {
    if (this.totalElements() === 0) {
      return '0 résultat';
    }
    const first = this.page() * this.size() + 1;
    const last = Math.min(first + this.size() - 1, this.totalElements());
    return `${first}–${last}`;
  });
}
