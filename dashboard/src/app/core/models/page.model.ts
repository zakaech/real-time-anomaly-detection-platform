/**
 * A page as the backend serialises it.
 *
 * Not Spring's own `Page` shape: that is an implementation detail of Spring
 * Data whose JSON has changed between versions. The backend wraps it in its own
 * record precisely so this contract is stable.
 */
export interface PageResponse<T> {
  readonly content: readonly T[];
  readonly page: number;
  readonly size: number;
  readonly totalElements: number;
  readonly totalPages: number;
  readonly last: boolean;
}
