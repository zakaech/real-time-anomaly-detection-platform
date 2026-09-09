/**
 * The sensor curve of one machine (decision D-41).
 *
 * Every value comes from a window the pipeline actually scored and published.
 * The nullable fields are the important part of this contract:
 *
 * - a sensor mean is null when that sensor produced no reading, so the chart
 *   must BREAK THE LINE rather than draw through the gap;
 * - the score fields are null on a window the model did not score, which is a
 *   normal state and not an error.
 *
 * Nothing here is interpolated, smoothed or back-filled.
 */
export interface TelemetrySeries {
  readonly machineCode: string;
  readonly lineCode: string;
  readonly from: string;
  readonly to: string;
  /** Window length in seconds, read from the data rather than assumed. */
  readonly windowSeconds: number;
  /** True when the range held more windows than the limit allowed. */
  readonly truncated: boolean;
  readonly points: readonly TelemetryPoint[];
}

export interface TelemetryPoint {
  readonly windowStart: string;
  readonly windowEnd: string;
  /** How full the window was; a low count means a partial aggregate. */
  readonly sampleCount: number;
  readonly machineState: string;
  readonly scored: boolean;
  readonly anomalyScore: number | null;
  readonly scoreThreshold: number | null;
  readonly anomaly: boolean | null;
  readonly temperatureCMean: number | null;
  readonly vibrationMmSMean: number | null;
  readonly pressureBarMean: number | null;
  readonly powerKwMean: number | null;
  readonly rotationRpmMean: number | null;
  /** Share of missing readings; what explains a gap in the curve. */
  readonly nullRatio: number | null;
}

/** The five physical signals the backend persists, with display metadata. */
export const SENSOR_SIGNALS = [
  { key: 'temperatureCMean', label: 'Température', unit: '°C', colour: '#e11d48' },
  { key: 'vibrationMmSMean', label: 'Vibration', unit: 'mm/s', colour: '#7c3aed' },
  { key: 'pressureBarMean', label: 'Pression', unit: 'bar', colour: '#0891b2' },
  { key: 'powerKwMean', label: 'Puissance', unit: 'kW', colour: '#ea580c' },
  { key: 'rotationRpmMean', label: 'Rotation', unit: 'rpm', colour: '#15803d' },
] as const;

export type SensorKey = (typeof SENSOR_SIGNALS)[number]['key'];
