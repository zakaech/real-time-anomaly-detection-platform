import { HttpClient } from '@angular/common/http';
import { inject } from '@angular/core';
import { firstValueFrom } from 'rxjs';
import { AppConfig, FALLBACK_CONFIG } from './app-config';

/**
 * Reads `assets/config.json` once, before the first component exists.
 *
 * It runs as an app initializer rather than lazily inside a service so that no
 * component can ever render against a half-loaded configuration -- the failure
 * that produces a dashboard briefly pointing at the wrong API.
 */
export async function loadAppConfig(): Promise<AppConfig> {
  const http = inject(HttpClient);
  try {
    const loaded = await firstValueFrom(http.get<Partial<AppConfig>>('assets/config.json'));
    return { ...FALLBACK_CONFIG, ...loaded };
  } catch {
    // A missing config file must not stop the application: the fallback keeps
    // the API path relative, which is correct behind the proxy anyway.
    console.warn('[config] assets/config.json unreadable; using defaults');
    return FALLBACK_CONFIG;
  }
}
