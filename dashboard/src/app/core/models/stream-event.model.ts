import { AlertSeverity, AlertStatus } from './alert.model';

/**
 * The payload of an SSE event.
 *
 * Narrower than `AlertSummary` on purpose: the stream says something happened
 * and gives enough to render a row. Anything more is fetched over REST, because
 * the stream is an accelerator for the display and never a source of truth.
 */
export interface AlertStreamEvent {
  readonly alertId: string;
  /**
   * Monotonic publication order, and the value the SSE `id:` line carries.
   * The alert id is a random UUID and cannot order anything, which is exactly
   * why this field exists on the backend.
   */
  readonly eventSeq: number;
  readonly machineCode: string;
  readonly lineCode: string;
  readonly severity: AlertSeverity;
  readonly status: AlertStatus;
  readonly anomalyScore: number;
  readonly detectedAt: string;
}

/** The named events the backend emits. `heartbeat` carries no payload. */
export type StreamEventName = 'alert.created' | 'alert.updated' | 'heartbeat';

/**
 * What the indicator shows.
 *
 * `reconnecting` is distinct from `error` deliberately: the first is a normal,
 * self-healing state and the second is not, and showing them the same way would
 * either cry wolf or hide a real outage.
 */
export type ConnectionState =
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'disconnected'
  | 'error';
