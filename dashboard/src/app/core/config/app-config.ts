import { InjectionToken } from '@angular/core';

/**
 * Runtime configuration, loaded before the application bootstraps.
 *
 * Deliberately NOT an Angular environment file. An environment file is baked in
 * at build time, which would mean one image per environment and a rebuild to
 * change a URL. This is read from `assets/config.json`, which the container
 * rewrites at startup, so the same image runs anywhere.
 */
export interface AppConfig {
  /**
   * Base path of the API. Relative by default (`/api/v1`) because the dashboard
   * is served behind the same origin as the backend: nginx in production, the
   * dev-server proxy in development. Same-origin removes the CORS problem
   * instead of working around it, and needs no backend change (decision D-42).
   */
  readonly apiBaseUrl: string;
  /** Ceiling for the live feed, so an unbounded stream cannot grow the DOM forever. */
  readonly liveFeedLimit: number;
  /** Initial reconnect delay; it doubles up to sseMaxBackoffMs. */
  readonly sseInitialBackoffMs: number;
  readonly sseMaxBackoffMs: number;
}

export const APP_CONFIG = new InjectionToken<AppConfig>('APP_CONFIG');

/**
 * Used only if `assets/config.json` cannot be read.
 *
 * The values are shapes, not secrets or hosts: the API path stays relative, so
 * even the fallback cannot point the dashboard at somebody else's backend.
 */
export const FALLBACK_CONFIG: AppConfig = {
  apiBaseUrl: '/api/v1',
  liveFeedLimit: 100,
  sseInitialBackoffMs: 1000,
  sseMaxBackoffMs: 30000,
};
