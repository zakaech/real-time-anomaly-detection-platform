import {
  ApplicationConfig,
  provideAppInitializer,
  provideBrowserGlobalErrorListeners,
  provideZonelessChangeDetection,
  inject,
} from '@angular/core';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { provideRouter, withComponentInputBinding } from '@angular/router';
import { APP_CONFIG, AppConfig } from './core/config/app-config';
import { loadAppConfig } from './core/config/app-config.loader';
import { httpErrorInterceptor } from './core/error/http-error.interceptor';
import { routes } from './app.routes';

/**
 * Configuration is loaded BEFORE anything renders.
 *
 * A service that fetched it lazily would let a component issue its first
 * request against a half-initialised configuration -- the failure that produces
 * a dashboard briefly pointing at the wrong API.
 */
let resolvedConfig: AppConfig | null = null;

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideZonelessChangeDetection(),
    provideRouter(routes, withComponentInputBinding()),
    provideHttpClient(withInterceptors([httpErrorInterceptor])),
    provideAppInitializer(async () => {
      resolvedConfig = await loadAppConfig();
    }),
    {
      provide: APP_CONFIG,
      useFactory: () => {
        if (!resolvedConfig) {
          throw new Error('APP_CONFIG requested before the initializer ran');
        }
        return resolvedConfig;
      },
    },
  ],
};
