import { Pipe, PipeTransform } from '@angular/core';

/**
 * Formatting lives in pipes, not in templates.
 *
 * A template that computes is a template nobody can unit-test; a pure pipe is
 * both testable and memoised by Angular.
 */
@Pipe({ name: 'score' })
export class ScorePipe implements PipeTransform {
  /** Scores are calibrated in [0,1] and six decimals is what distinguishes them. */
  transform(value: number | null | undefined): string {
    return value === null || value === undefined ? '—' : value.toFixed(6);
  }
}

@Pipe({ name: 'shortTime' })
export class ShortTimePipe implements PipeTransform {
  transform(iso: string | null | undefined): string {
    if (!iso) {
      return '—';
    }
    const date = new Date(iso);
    return Number.isNaN(date.getTime()) ? '—' : date.toLocaleTimeString();
  }
}

@Pipe({ name: 'dateTime' })
export class DateTimePipe implements PipeTransform {
  transform(iso: string | null | undefined): string {
    if (!iso) {
      return '—';
    }
    const date = new Date(iso);
    return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString();
  }
}

@Pipe({ name: 'durationSeconds' })
export class DurationSecondsPipe implements PipeTransform {
  /** Duration between two instants, as a human reads it. */
  transform(fromIso: string | null | undefined, toIso: string | null | undefined): string {
    if (!fromIso || !toIso) {
      return '—';
    }
    const seconds = Math.round((new Date(toIso).getTime() - new Date(fromIso).getTime()) / 1000);
    if (!Number.isFinite(seconds)) {
      return '—';
    }
    if (seconds < 60) {
      return `${seconds} s`;
    }
    const minutes = Math.floor(seconds / 60);
    const rest = seconds % 60;
    return rest === 0 ? `${minutes} min` : `${minutes} min ${rest} s`;
  }
}
