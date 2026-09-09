/**
 * Mirrors the DTOs the backend actually serialises.
 *
 * Every field here was read off a real response, not inferred from the Java
 * source: a contract you have not seen on the wire is a guess. The one place the
 * payload is not camelCase is `z_score`, and it is reproduced faithfully rather
 * than tidied up (decision D-45) -- renaming it in the model would mean the
 * model no longer describes the wire.
 */

export type AlertSeverity = 'MEDIUM' | 'HIGH' | 'CRITICAL';

/**
 * All four values exist in the database CHECK constraint. Only NEW and
 * ACKNOWLEDGED are reachable today; the other two are declared so that adding
 * them to the backend does not make this a breaking change.
 */
export type AlertStatus = 'NEW' | 'ACKNOWLEDGED' | 'RESOLVED' | 'DISMISSED';

/** Severities in escalating order, for sorting and for filter menus. */
export const SEVERITY_ORDER: readonly AlertSeverity[] = ['MEDIUM', 'HIGH', 'CRITICAL'] as const;
export const STATUS_ORDER: readonly AlertStatus[] = [
  'NEW',
  'ACKNOWLEDGED',
  'RESOLVED',
  'DISMISSED',
] as const;

/** One row of the alert list. */
export interface AlertSummary {
  readonly alertId: string;
  readonly machineCode: string;
  readonly lineCode: string;
  readonly severity: AlertSeverity;
  readonly status: AlertStatus;
  readonly anomalyScore: number;
  readonly scoreThreshold: number;
  /** ISO-8601 UTC. Event time: when the machine deviated, not when we decided. */
  readonly detectedAt: string;
  readonly consecutiveWindows: number;
}

/**
 * One feature that departed most from the training reference.
 *
 * `z_score` really is snake_case on the wire, inside an otherwise camelCase
 * payload: the backend reuses one DTO for the Kafka message and the REST
 * response, and the Kafka side is snake_case.
 */
export interface Contributor {
  readonly feature: string;
  readonly z_score: number;
}

export interface ModelInfo {
  readonly name: string;
  readonly version: string;
  readonly trainedAt: string | null;
  /** Ties the alert to the exact artefact that scored it. */
  readonly artifactSha256: string | null;
}

export interface AcknowledgementEntry {
  readonly previousStatus: AlertStatus;
  readonly newStatus: AlertStatus;
  readonly actor: string;
  readonly comment: string | null;
  readonly occurredAt: string;
}

/** Everything the platform holds about one alert. */
export interface AlertDetail extends AlertSummary {
  readonly machineType: string | null;
  readonly windowStart: string;
  readonly windowEnd: string;
  readonly publishedAt: string;
  readonly ingestedAt: string;
  readonly model: ModelInfo;
  readonly topContributors: readonly Contributor[];
  readonly history: readonly AcknowledgementEntry[];
  /** Optimistic-lock version, sent back as `expectedVersion` when acknowledging. */
  readonly version: number;
}

export interface AcknowledgeRequest {
  readonly comment?: string;
  /**
   * Optional by design (D-09). Sent when the client wants the guarantee that
   * nobody else changed the alert first; a mismatch is a 409 rather than a
   * silent overwrite.
   */
  readonly expectedVersion?: number;
}
