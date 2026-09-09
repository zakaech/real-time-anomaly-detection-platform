import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { catchError, throwError } from 'rxjs';
import { ApiError, ProblemDetail } from '../models/problem-detail.model';

/**
 * Turns every HTTP failure into one typed error.
 *
 * Without this, each component would have to know that the backend speaks
 * RFC 9457 and would dig into `error.error.detail` by hand -- and the ones that
 * forgot would show the browser's generic message instead of the reason the
 * server gave.
 *
 * The parsed body is kept rather than flattened to a string: a 409 carries
 * `currentStatus` and `currentVersion`, which is what lets the acknowledgement
 * form tell the operator what actually happened.
 */
export const httpErrorInterceptor: HttpInterceptorFn = (request, next) =>
  next(request).pipe(
    catchError((error: unknown) => {
      if (!(error instanceof HttpErrorResponse)) {
        return throwError(() => error);
      }

      const problem = asProblemDetail(error.error);

      // Status 0 means the request never reached the server -- offline, DNS,
      // the proxy being down. It reads differently from a server error and the
      // UI says so instead of blaming the backend.
      const message =
        error.status === 0
          ? 'Le serveur est injoignable'
          : (problem?.detail ?? error.message);

      return throwError(() => new ApiError(error.status, problem, message));
    }),
  );

/**
 * A ProblemDetail, or null if the body is something else.
 *
 * Checked structurally rather than assumed: a proxy returning an HTML error
 * page would otherwise be read as a problem document and produce
 * `undefined` where a message is expected.
 */
function asProblemDetail(body: unknown): ProblemDetail | null {
  if (body === null || typeof body !== 'object') {
    return null;
  }
  const candidate = body as Partial<ProblemDetail>;
  if (typeof candidate.title !== 'string' || typeof candidate.status !== 'number') {
    return null;
  }
  return candidate as ProblemDetail;
}
