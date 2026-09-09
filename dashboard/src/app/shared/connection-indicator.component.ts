import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { ConnectionState } from '../core/models/stream-event.model';

/**
 * Whether the live channel is actually live.
 *
 * This is not decoration. Without traffic a proxy closes an idle SSE connection
 * after 30-60 seconds, and a dashboard that stopped updating while still looking
 * connected is the most deceptive failure a live display has. `reconnecting` is
 * shown differently from `error` because the first heals itself and the second
 * does not.
 */
@Component({
  selector: 'app-connection-indicator',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <span class="indicator" [class]="state()" role="status" [attr.aria-live]="'polite'">
      <span class="dot" aria-hidden="true"></span>
      {{ text() }}
    </span>
  `,
  styles: [
    `
      .indicator {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        font-size: 0.8rem;
        font-weight: 600;
      }
      .dot {
        width: 0.5rem;
        height: 0.5rem;
        border-radius: 50%;
        background: currentColor;
      }
      .connected {
        color: #15803d;
      }
      .connecting,
      .reconnecting {
        color: #b45309;
      }
      .connecting .dot,
      .reconnecting .dot {
        animation: pulse 1.2s ease-in-out infinite;
      }
      .disconnected {
        color: #64748b;
      }
      .error {
        color: #b91c1c;
      }
      @keyframes pulse {
        50% {
          opacity: 0.25;
        }
      }
      @media (prefers-reduced-motion: reduce) {
        .dot {
          animation: none;
        }
      }
    `,
  ],
})
export class ConnectionIndicatorComponent {
  readonly state = input.required<ConnectionState>();

  private static readonly LABELS: Record<ConnectionState, string> = {
    connecting: 'Connexion...',
    connected: 'En direct',
    reconnecting: 'Reconnexion...',
    disconnected: 'Déconnecté',
    error: 'Erreur de flux',
  };

  protected readonly text = computed(() => ConnectionIndicatorComponent.LABELS[this.state()]);
}
