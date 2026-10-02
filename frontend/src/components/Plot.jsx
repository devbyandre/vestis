// Plotly with only the trace types Vestis draws. The full plotly.js bundle is
// ~4.5 MB; this registers scatter, bar, pie, histogram, treemap and candlestick.
import Plotly from 'plotly.js/lib/core'
import scatter from 'plotly.js/lib/scatter'
import bar from 'plotly.js/lib/bar'
import pie from 'plotly.js/lib/pie'
import histogram from 'plotly.js/lib/histogram'
import treemap from 'plotly.js/lib/treemap'
import candlestick from 'plotly.js/lib/candlestick'
import createPlotlyComponent from 'react-plotly.js/factory'

Plotly.register([scatter, bar, pie, histogram, treemap, candlestick])

export default createPlotlyComponent(Plotly)
