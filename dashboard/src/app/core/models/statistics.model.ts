import { AlertSeverity, AlertStatus } from './alert.model';

/**
 * Dashboard aggregates, all computed by PostgreSQL.
 *
 * The two maps are `Partial` on purpose: the backend emits a GROUP BY result, so
 * a severity with no alerts is an ABSENT KEY, not a zero. Typing them as total
 * records would make `stats.bySeverity.MEDIUM` look like a number when it is
 * `undefined` -- verified on a real response, where only the severities present
 * in the range appear.
 */
export interface AlertStatistics {
  readonly from: string;
  readonly to: string;
  readonly total: number;
  readonly bySeverity: Partial<Record<AlertSeverity, number>>;
  readonly byStatus: Partial<Record<AlertStatus, number>>;
  readonly byMachine: readonly MachineCount[];
  readonly overTime: readonly TimeBucket[];
  readonly acknowledgementRate: number;
  /** Null when nothing has been acknowledged in the range. */
  readonly medianSecondsToAcknowledge: number | null;
}

export interface MachineCount {
  readonly machineCode: string;
  readonly lineCode: string;
  readonly count: number;
}

export interface TimeBucket {
  readonly bucketStart: string;
  readonly count: number;
}

export type StatsGranularity = 'hour' | 'day';
