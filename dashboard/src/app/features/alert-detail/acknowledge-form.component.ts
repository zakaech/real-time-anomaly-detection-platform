import { ChangeDetectionStrategy, Component, input, output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

export interface AcknowledgeSubmission {
  readonly comment: string;
  readonly operator: string;
}

/**
 * The acknowledgement form.
 *
 * Presentational: it collects a comment and an operator name and emits them.
 * The 1000-character limit mirrors the backend constraint so an operator is told
 * before the request rather than by a 400 afterwards.
 *
 * The operator field exists because there is no authentication in v1 -- the
 * backend records whatever `X-Operator` carries. It is labelled as an audit
 * field rather than dressed up as a login, because presenting it as identity
 * would be claiming a guarantee the system does not provide.
 */
@Component({
  selector: 'app-acknowledge-form',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule],
  template: `
    <form class="ack" (submit)="submit($event)">
      <h3>Acquitter l'alerte</h3>

      <label>
        <span>Opérateur <em>(champ d'audit, non authentifié)</em></span>
        <input name="operator" [(ngModel)]="operator" maxlength="128" required />
      </label>

      <label>
        <span>Commentaire</span>
        <textarea
          name="comment"
          [(ngModel)]="comment"
          rows="3"
          [maxlength]="maxComment"
          placeholder="Ce qui a été constaté ou fait"
        ></textarea>
        <small [class.over]="comment.length > maxComment">
          {{ comment.length }} / {{ maxComment }}
        </small>
      </label>

      @if (conflict()) {
        <p class="conflict" role="alert">
          {{ conflict() }}
        </p>
      }

      <button type="submit" [disabled]="busy() || !operator.trim()">
        {{ busy() ? 'Envoi...' : 'Acquitter' }}
      </button>
    </form>
  `,
  styles: [
    `
      .ack {
        display: flex;
        flex-direction: column;
        gap: 0.7rem;
        padding: 1rem;
        background: #fff;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
      }
      h3 {
        margin: 0;
        font-size: 0.95rem;
      }
      label {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
        font-size: 0.8rem;
        color: #475569;
      }
      em {
        font-style: normal;
        color: #94a3b8;
      }
      input,
      textarea {
        padding: 0.45rem 0.55rem;
        border: 1px solid #cbd5e1;
        border-radius: 6px;
        font: inherit;
      }
      small {
        align-self: flex-end;
        color: #94a3b8;
      }
      small.over {
        color: #b91c1c;
      }
      .conflict {
        margin: 0;
        padding: 0.55rem 0.7rem;
        background: #fef3c7;
        border: 1px solid #fcd34d;
        border-radius: 6px;
        color: #92400e;
        font-size: 0.85rem;
      }
      button {
        align-self: flex-start;
        padding: 0.5rem 1.1rem;
        background: #15803d;
        color: #fff;
        border: none;
        border-radius: 6px;
        font-weight: 600;
        cursor: pointer;
      }
      button:disabled {
        background: #94a3b8;
        cursor: not-allowed;
      }
    `,
  ],
})
export class AcknowledgeFormComponent {
  readonly busy = input(false);
  /** Set when the server refused because the alert moved under us (409). */
  readonly conflict = input<string | null>(null);
  readonly acknowledge = output<AcknowledgeSubmission>();

  protected readonly maxComment = 1000;
  protected operator = 'operateur';
  protected comment = '';

  private readonly submitted = signal(false);

  protected submit(event: Event): void {
    event.preventDefault();
    if (!this.operator.trim() || this.comment.length > this.maxComment) {
      return;
    }
    this.submitted.set(true);
    this.acknowledge.emit({ comment: this.comment.trim(), operator: this.operator.trim() });
  }
}
