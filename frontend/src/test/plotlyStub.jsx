// Test-only stand-in for components/Plot (the @plot alias) — jsdom has no canvas/WebGL, and
// smoke tests only need to know the Tab rendered without throwing, not that
// the chart actually paints.
export default function PlotlyStub() {
  return <div data-testid="plotly-stub" />
}
