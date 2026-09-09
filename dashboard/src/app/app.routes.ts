import { Routes } from '@angular/router';

/**
 * Three routes, each lazily loaded.
 *
 * Lazy loading is not premature here: the detail page pulls in Chart.js, and a
 * user who only watches the live feed should not download a charting library to
 * do it.
 */
export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'live' },
  {
    path: 'live',
    title: 'Temps réel',
    loadComponent: () =>
      import('./features/live/live-page.component').then((m) => m.LivePageComponent),
  },
  {
    path: 'history',
    title: 'Historique',
    loadComponent: () =>
      import('./features/history/history-page.component').then((m) => m.HistoryPageComponent),
  },
  {
    path: 'alerts/:alertId',
    title: 'Détail alerte',
    loadComponent: () =>
      import('./features/alert-detail/alert-detail-page.component').then(
        (m) => m.AlertDetailPageComponent,
      ),
  },
  { path: '**', redirectTo: 'live' },
];
