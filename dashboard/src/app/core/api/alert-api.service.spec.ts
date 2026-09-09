import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { APP_CONFIG, FALLBACK_CONFIG } from '../config/app-config';
import { DEFAULT_QUERY } from '../models/filters.model';
import { AlertApiService } from './alert-api.service';

describe('AlertApiService', () => {
  let service: AlertApiService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: APP_CONFIG, useValue: FALLBACK_CONFIG },
      ],
    });
    service = TestBed.inject(AlertApiService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('sends paging and sorting to the server', () => {
    service.listAlerts(DEFAULT_QUERY).subscribe();

    const request = http.expectOne((r) => r.url === '/api/v1/alerts');
    expect(request.request.params.get('page')).toBe('0');
    expect(request.request.params.get('size')).toBe('20');
    // The backend expects one `property,direction` string, not two params.
    expect(request.request.params.get('sort')).toBe('detectedAt,desc');
    request.flush({ content: [], page: 0, size: 20, totalElements: 0, totalPages: 0, last: true });
  });

  it('repeats severity and status parameters rather than joining them', () => {
    service
      .listAlerts({ ...DEFAULT_QUERY, severity: ['HIGH', 'CRITICAL'], status: ['NEW'] })
      .subscribe();

    const request = http.expectOne((r) => r.url === '/api/v1/alerts');
    // The backend binds a List from repeated parameters; a comma-joined string
    // would arrive as one bogus enum value.
    expect(request.request.params.getAll('severity')).toEqual(['HIGH', 'CRITICAL']);
    expect(request.request.params.getAll('status')).toEqual(['NEW']);
    request.flush({ content: [], page: 0, size: 20, totalElements: 0, totalPages: 0, last: true });
  });

  it('omits filters that were not filled in', () => {
    service.listAlerts(DEFAULT_QUERY).subscribe();

    const request = http.expectOne((r) => r.url === '/api/v1/alerts');
    // Sending machineCode='' would filter on the empty string and return
    // nothing, which looks like "no data" rather than a mistake.
    expect(request.request.params.has('machineCode')).toBe(false);
    expect(request.request.params.has('from')).toBe(false);
    request.flush({ content: [], page: 0, size: 20, totalElements: 0, totalPages: 0, last: true });
  });

  it('caps the page size at the documented maximum', () => {
    service.listAlerts({ ...DEFAULT_QUERY, size: 5000 }).subscribe();

    const request = http.expectOne((r) => r.url === '/api/v1/alerts');
    // The backend answers 400 above 100; clamping turns a broken screen into a
    // capped page.
    expect(request.request.params.get('size')).toBe('100');
    request.flush({ content: [], page: 0, size: 100, totalElements: 0, totalPages: 0, last: true });
  });

  it('sends the operator header when acknowledging', () => {
    service.acknowledge('abc', { comment: 'checked' }, 'op.martin').subscribe();

    const request = http.expectOne('/api/v1/alerts/abc/acknowledge');
    expect(request.request.method).toBe('POST');
    expect(request.request.headers.get('X-Operator')).toBe('op.martin');
    expect(request.request.body).toEqual({ comment: 'checked' });
    request.flush({});
  });

  it('requests telemetry for an explicit range', () => {
    service.getTelemetry('M-011', '2026-09-09T10:00:00Z', '2026-09-09T11:00:00Z', 500).subscribe();

    const request = http.expectOne((r) => r.url === '/api/v1/machines/M-011/telemetry');
    expect(request.request.params.get('from')).toBe('2026-09-09T10:00:00Z');
    expect(request.request.params.get('maxPoints')).toBe('500');
    request.flush({ points: [] });
  });
});
