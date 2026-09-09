import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { APP_CONFIG } from '../config/app-config';
import {
  AcknowledgeRequest,
  AlertDetail,
  AlertSummary,
} from '../models/alert.model';
import { AlertQuery, MAX_PAGE_SIZE } from '../models/filters.model';
import { PageResponse } from '../models/page.model';
import { AlertStatistics, StatsGranularity } from '../models/statistics.model';
import { TelemetrySeries } from '../models/telemetry.model';

/**
 * The only place that knows the shape of the API.
 *
 * Components never build a URL or a parameter: a query string assembled in a
 * template is a contract nobody can find when the API changes.
 */
@Injectable({ providedIn: 'root' })
export class AlertApiService {
  private readonly http = inject(HttpClient);
  private readonly config = inject(APP_CONFIG);

  /**
   * One page of alerts.
   *
   * Filters, sort and paging all go to the server. The size is clamped here as
   * well as there -- not because the backend cannot be trusted, but because a
   * 400 from a slider that went too far is a worse experience than a capped
   * page, and the cap is the documented behaviour either way.
   */
  listAlerts(query: AlertQuery): Observable<PageResponse<AlertSummary>> {
    let params = new HttpParams()
      .set('page', query.page)
      .set('size', Math.min(query.size, MAX_PAGE_SIZE))
      .set('sort', `${query.sort},${query.direction}`);

    if (query.machineCode) {
      params = params.set('machineCode', query.machineCode);
    }
    if (query.lineCode) {
      params = params.set('lineCode', query.lineCode);
    }
    // Repeated parameters, which is what the backend binds to a List.
    for (const severity of query.severity ?? []) {
      params = params.append('severity', severity);
    }
    for (const status of query.status ?? []) {
      params = params.append('status', status);
    }
    if (query.from) {
      params = params.set('from', query.from);
    }
    if (query.to) {
      params = params.set('to', query.to);
    }

    return this.http.get<PageResponse<AlertSummary>>(`${this.config.apiBaseUrl}/alerts`, {
      params,
    });
  }

  getAlert(alertId: string): Observable<AlertDetail> {
    return this.http.get<AlertDetail>(
      `${this.config.apiBaseUrl}/alerts/${encodeURIComponent(alertId)}`,
    );
  }

  /**
   * Acknowledge an alert.
   *
   * `X-Operator` is an audit field, not authentication -- the backend has none
   * in v1 and records whatever it is given. Sending it from here keeps the
   * audit trail populated with something more useful than "unknown".
   */
  acknowledge(
    alertId: string,
    request: AcknowledgeRequest,
    operator: string,
  ): Observable<AlertDetail> {
    return this.http.post<AlertDetail>(
      `${this.config.apiBaseUrl}/alerts/${encodeURIComponent(alertId)}/acknowledge`,
      request,
      { headers: { 'X-Operator': operator } },
    );
  }

  getStatistics(
    from?: string,
    to?: string,
    granularity: StatsGranularity = 'hour',
  ): Observable<AlertStatistics> {
    let params = new HttpParams().set('granularity', granularity);
    if (from) {
      params = params.set('from', from);
    }
    if (to) {
      params = params.set('to', to);
    }
    return this.http.get<AlertStatistics>(`${this.config.apiBaseUrl}/alerts/stats`, { params });
  }

  /**
   * The sensor curve of one machine (D-41).
   *
   * The range is always explicit. An open-ended request would let a chart ask
   * for the whole table, and the backend would cap it silently.
   */
  getTelemetry(
    machineCode: string,
    from: string,
    to: string,
    maxPoints?: number,
  ): Observable<TelemetrySeries> {
    let params = new HttpParams().set('from', from).set('to', to);
    if (maxPoints !== undefined) {
      params = params.set('maxPoints', maxPoints);
    }
    return this.http.get<TelemetrySeries>(
      `${this.config.apiBaseUrl}/machines/${encodeURIComponent(machineCode)}/telemetry`,
      { params },
    );
  }
}
