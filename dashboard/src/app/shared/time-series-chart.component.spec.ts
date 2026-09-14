import { Chart } from 'chart.js';
import { describe, expect, it } from 'vitest';
// Imported for its side effect: this module is where Chart.js is configured.
import './time-series-chart.component';

/**
 * Chart.js 4 is tree-shakable: a chart type that is not registered does not
 * degrade, it throws `"<type>" is not a registered controller` at construction
 * and leaves an empty canvas. That is what happened to the contributor chart --
 * a `bar` chart -- while only line and scatter were registered, and no test
 * caught it because the telemetry chart, a line chart, kept working.
 *
 * Rendering a chart under jsdom cannot reproduce it: with no 2D context Chart.js
 * stops before it ever reaches the registry. So the registry is asked directly,
 * with the same call the failure came from, for every chart type the dashboard
 * builds.
 */
describe('Chart.js registration', () => {
  it.each(['line', 'scatter', 'bar'])('registers the %s controller the dashboard uses', (type) => {
    expect(() => Chart.registry.getController(type)).not.toThrow();
  });

  it.each(['line', 'point', 'bar'])('registers the %s element those charts draw with', (type) => {
    expect(() => Chart.registry.getElement(type)).not.toThrow();
  });
});
