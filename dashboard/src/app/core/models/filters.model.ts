import { AlertSeverity, AlertStatus } from './alert.model';

/**
 * The query the list endpoint accepts.
 *
 * Every one of these is sent to the API and executed by PostgreSQL. None is
 * applied in the browser: filtering a page of twenty rows client-side would give
 * an answer that disagrees with `totalElements` on the very same response.
 */
export interface AlertFilters {
  readonly machineCode?: string;
  readonly lineCode?: string;
  readonly severity?: readonly AlertSeverity[];
  readonly status?: readonly AlertStatus[];
  /** ISO-8601 instants; the backend refuses an inverted range with a 400. */
  readonly from?: string;
  readonly to?: string;
}

/** Only the properties the backend whitelists for sorting. */
export type AlertSortProperty = 'detectedAt' | 'severity' | 'anomalyScore' | 'status';
export type SortDirection = 'asc' | 'desc';

export interface AlertQuery extends AlertFilters {
  readonly page: number;
  readonly size: number;
  readonly sort: AlertSortProperty;
  readonly direction: SortDirection;
}

/** The backend caps size at 100 and rejects anything larger with a 400. */
export const MAX_PAGE_SIZE = 100;
export const DEFAULT_PAGE_SIZE = 20;

export const DEFAULT_QUERY: AlertQuery = {
  page: 0,
  size: DEFAULT_PAGE_SIZE,
  sort: 'detectedAt',
  direction: 'desc',
};
