import { ChangeDetectionStrategy, Component, OnDestroy, OnInit, inject } from '@angular/core';
import { AlertStoreService } from '../../core/store/alert-store.service';
import { ConnectionIndicatorComponent } from '../../shared/connection-indicator.component';
import {
  EmptyStateComponent,
  ErrorStateComponent,
  LoadingStateComponent,
} from '../../shared/state-views.component';
import { AlertFeedComponent } from './alert-feed.component';
import { SeverityCountersComponent } from './severity-counters.component';

/**
 * The live view: counters, connection state, and the incoming feed.
 *
 * A container. It owns no rendering logic of its own -- it wires the store to
 * three presentational components and decides which of loading, error, empty or
 * data is on screen.
 */
@Component({
  selector: 'app-live-page',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    SeverityCountersComponent,
    AlertFeedComponent,
    ConnectionIndicatorComponent,
    LoadingStateComponent,
    EmptyStateComponent,
    ErrorStateComponent,
  ],
  template: `
    <section class="page">
      <header class="page-header">
        <div>
          <h2>Flux temps réel</h2>
          <p class="subtitle">
            Alertes poussées par le serveur. Le flux accélère l'affichage&nbsp;; la source de
            vérité reste PostgreSQL.
          </p>
        </div>
        <app-connection-indicator [state]="store.connectionState()" />
      </header>

      <app-severity-counters
        [total]="store.total()"
        [newCount]="store.newCount()"
        [counts]="store.countsBySeverity()"
      />

      @if (store.error(); as error) {
        <app-error-state
          title="Impossible de charger les alertes"
          [message]="error"
          (retry)="store.refresh()"
        />
      } @else if (store.loading() && store.total() === 0) {
        <app-loading-state message="Chargement des alertes..." />
      } @else if (store.total() === 0) {
        <app-empty-state
          title="Aucune alerte"
          hint="Les nouvelles alertes apparaîtront ici sans rafraîchissement."
        />
      } @else {
        <app-alert-feed [alerts]="store.feed()" />
      }
    </section>
  `,
  styles: [
    `
      .page-header {
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
        align-items: flex-start;
        justify-content: space-between;
        margin-bottom: 1rem;
      }
      h2 {
        margin: 0;
        font-size: 1.2rem;
      }
      .subtitle {
        margin: 0.2rem 0 0;
        font-size: 0.85rem;
        color: #64748b;
        max-width: 60ch;
      }
    `,
  ],
})
export class LivePageComponent implements OnInit, OnDestroy {
  protected readonly store = inject(AlertStoreService);

  ngOnInit(): void {
    this.store.start();
  }

  ngOnDestroy(): void {
    // Leaving the page closes the channel. Without this a user navigating back
    // and forth would accumulate open connections against the server.
    this.store.stop();
  }
}
