/**
 * RFC 9457 error body, as the backend produces it.
 *
 * `traceId` is on every error and is correlated with the server logs, so a
 * screenshot from an operator leads to the right log line. A 409 additionally
 * carries the current state, which lets the client resynchronise without a
 * second request.
 */
export interface ProblemDetail {
  readonly type: string;
  readonly title: string;
  readonly status: number;
  readonly detail: string;
  readonly instance?: string;
  readonly traceId: string;
  readonly alertId?: string;
  readonly machineCode?: string;
  readonly currentStatus?: string;
  readonly currentVersion?: number;
}

/**
 * What the application throws once an HTTP failure has been interpreted.
 *
 * Carrying the parsed `ProblemDetail` rather than a bare status is what lets a
 * component show the current status on a 409 instead of a generic message.
 */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly problem: ProblemDetail | null,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }

  /** True when the alert changed under us; the caller should reload it. */
  get isConflict(): boolean {
    return this.status === 409;
  }

  get isNotFound(): boolean {
    return this.status === 404;
  }

  /** Zero when the browser never reached the server, which reads differently. */
  get isNetwork(): boolean {
    return this.status === 0;
  }
}
