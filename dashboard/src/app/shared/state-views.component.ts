import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';

/**
 * The three states every data view has to show.
 *
 * They live together because they are the same decision seen three ways, and
 * because a page that forgets one of them is a page that shows an empty table
 * when the server is down.
 */
@Component({
  selector: 'app-loading-state',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="state" role="status" aria-live="polite">
      <span class="spinner" aria-hidden="true"></span>
      <span>{{ message() }}</span>
    </div>
  `,
  styles: [
    `
      .state {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 0.6rem;
        padding: 2.5rem 1rem;
        color: #475569;
      }
      .spinner {
        width: 1rem;
        height: 1rem;
        border: 2px solid #cbd5e1;
        border-top-color: #475569;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
      }
      @keyframes spin {
        to {
          transform: rotate(360deg);
        }
      }
      @media (prefers-reduced-motion: reduce) {
        .spinner {
          animation: none;
        }
      }
    `,
  ],
})
export class LoadingStateComponent {
  readonly message = input('Chargement...');
}

@Component({
  selector: 'app-empty-state',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="state">
      <p class="title">{{ title() }}</p>
      @if (hint()) {
        <p class="hint">{{ hint() }}</p>
      }
    </div>
  `,
  styles: [
    `
      .state {
        padding: 2.5rem 1rem;
        text-align: center;
        color: #64748b;
      }
      .title {
        margin: 0;
        font-weight: 600;
      }
      .hint {
        margin: 0.35rem 0 0;
        font-size: 0.85rem;
      }
    `,
  ],
})
export class EmptyStateComponent {
  readonly title = input('Aucun résultat');
  readonly hint = input<string | null>(null);
}

@Component({
  selector: 'app-error-state',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="state" role="alert">
      <p class="title">{{ title() }}</p>
      <p class="detail">{{ message() }}</p>
      @if (traceId()) {
        <!-- Shown so a screenshot leads to the matching server log line. -->
        <p class="trace">trace : {{ traceId() }}</p>
      }
      <button type="button" (click)="retry.emit()">Réessayer</button>
    </div>
  `,
  styles: [
    `
      .state {
        padding: 2rem 1rem;
        text-align: center;
        color: #991b1b;
        background: #fef2f2;
        border: 1px solid #fecaca;
        border-radius: 8px;
      }
      .title {
        margin: 0;
        font-weight: 700;
      }
      .detail {
        margin: 0.35rem 0;
        font-size: 0.9rem;
      }
      .trace {
        margin: 0 0 0.75rem;
        font-family: ui-monospace, monospace;
        font-size: 0.75rem;
        color: #7f1d1d;
      }
      button {
        padding: 0.4rem 1rem;
        border: 1px solid #b91c1c;
        background: #fff;
        color: #b91c1c;
        border-radius: 6px;
        cursor: pointer;
        font-weight: 600;
      }
    `,
  ],
})
export class ErrorStateComponent {
  readonly title = input('Une erreur est survenue');
  readonly message = input('');
  readonly traceId = input<string | null>(null);
  readonly retry = output<void>();
}
