## Time Series Anomaly Visualization – Guideline

This visualization is used consistently across the project to answer the
following standard questions for time series anomaly analysis.

### Standard Questions

1. Where in the time series do unusual deviations from typical signal behavior occur?

2. Do highlighted deviations appear as isolated events or as contiguous time segments?

3. How do highlighted deviations relate to the local signal shape
   (e.g., extremes, transitions, plateaus, or abrupt level changes)?

4. What are the exact timestamp and value of a detected anomaly when closer inspection is required?

### Visualization Constraints

- A single continuous line plot is used to show the full time series context.
- Highlighted markers are overlaid directly on the signal.
- No additional subplots, thresholds, or secondary axes are introduced.
- The same visual encoding is used across different detection methods.
- Interactive features (e.g., hover tooltips) are optional and may be used
  to support precise inspection of individual anomaly points without altering
  the overall visual structure.
