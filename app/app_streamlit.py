#!/usr/bin/env python3
# app_streamlit.py
import os, json, datetime as dt
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from scipy.stats import skew, kurtosis, norm
import matplotlib.pyplot as plt
from typing import List

import middleware as mw
import db_utils as db

from config_utils import safe_json_load, get_all_config, set_config, get_config

# mw.recompute_all_holdings_timeseries()
# mw.update_portfolio_risk_timeseries_for_portfolios()


st.set_page_config(page_title="Stock Analysis Manager", layout="wide")
st.title("Stock Analysis Manager")


# Securities
def format_sec_label(symbol):
    meta = mw.get_security_basic(symbol)
    # print(meta)
    if meta:
        # Prefer longName if it exists and is not empty, otherwise shortName, otherwise symbol
        name = meta.get('longName') or meta.get('shortName') or None
        if name and name.strip():  # avoid empty strings
            return f"{name} ({symbol})"
    return symbol

def display_kpi_table(df: pd.DataFrame):
    if df.empty:
        st.info("No watchlist items")
        return

    # --- Fill empty string fields ---
    string_cols = ["symbol", "security_name", "Temperature"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].fillna("N/A").astype(str)

    # --- Ensure numeric columns are floats ---
    numeric_cols = [
        "regularMarketPrice", "fiftyTwoWeekLow", "fiftyTwoWeekHigh",
        "beta", "trailingPE", "pb_ratio", "dividendYield", "profitMargins"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Compute Temperature based on key metrics ---
    def compute_temperature(row):
        score = 0

        # Beta: lower is better
        beta = row.get("beta")
        if pd.notna(beta):
            if beta < 1.0: score += 1
            elif beta <= 1.2: score += 0
            else: score -= 1

        # P/E: lower is better
        pe = row.get("trailingPE")
        if pd.notna(pe):
            if pe < 15: score += 1
            elif pe <= 25: score += 0
            else: score -= 1

        # P/B: lower is better
        pb = row.get("pb_ratio")
        if pd.notna(pb):
            if pb < 1.5: score += 1
            elif pb <= 3: score += 0
            else: score -= 1

        # Dividend Yield: higher is better
        div = row.get("dividendYield")
        if pd.notna(div):
            if div > 0.03: score += 1
            elif div >= 0.01: score += 0
            else: score -= 1

        # Profit Margin: higher is better
        pm = row.get("profitMargins")
        if pd.notna(pm):
            if pm > 0.2: score += 1
            elif pm >= 0.1: score += 0
            else: score -= 1

        # Assign Temperature label
        if score >= 3:
            return "Hot"
        elif score >= 1:
            return "Warm"
        else:
            return "Cold"

    df["Temperature"] = df.apply(compute_temperature, axis=1)

    # --- Color map for temperature ---
    def temp_color(val):
        if val == "Hot":
            return "background-color: #ff6666; color: white; font-weight: bold"
        elif val == "Warm":
            return "background-color: #ffcc66; color: black; font-weight: bold"
        elif val == "Cold":
            return "background-color: #66b3ff; color: white; font-weight: bold"
        else:
            return ""

    # --- Other color functions ---
    def beta_color(val):
        if pd.isna(val): return ""
        if val < 1.0: return "color: green; font-weight: bold"
        elif val <= 1.2: return "color: orange; font-weight: bold"
        else: return "color: red; font-weight: bold"

    def pe_color(val):
        if pd.isna(val): return ""
        if val < 15: return "color: green; font-weight: bold"
        elif val <= 25: return "color: orange; font-weight: bold"
        else: return "color: red; font-weight: bold"

    def pb_color(val):
        if pd.isna(val): return ""
        if val < 1.5: return "color: green; font-weight: bold"
        elif val <= 3: return "color: orange; font-weight: bold"
        else: return "color: red; font-weight: bold"

    def div_color(val):
        if pd.isna(val): return ""
        if val > 0.03: return "color: green; font-weight: bold"
        elif val >= 0.01: return "color: orange; font-weight: bold"
        else: return "color: red; font-weight: bold"

    def profit_color(val):
        if pd.isna(val): return ""
        if val > 0.2: return "color: green; font-weight: bold"
        elif val >= 0.1: return "color: orange; font-weight: bold"
        else: return "color: red; font-weight: bold"

    # --- Choose displayed columns ---
    display_cols = [
        "symbol", "security_name", "regularMarketPrice", "fiftyTwoWeekLow", "fiftyTwoWeekHigh",
        "beta", "trailingPE", "pb_ratio", "dividendYield", "profitMargins", "Temperature"
    ]
    df_display = df[display_cols]

    # --- Style the dataframe ---
    styled = df_display.style.format({
        "regularMarketPrice": "€{:,.2f}",
        "fiftyTwoWeekLow": "€{:,.2f}",
        "fiftyTwoWeekHigh": "€{:,.2f}",
        "trailingPE": "{:.1f}",
        "pb_ratio": "{:.2f}",
        "dividendYield": "{:.2%}",
        "profitMargins": "{:.2%}"
    }).map(temp_color, subset=["Temperature"]) \
      .map(beta_color, subset=["beta"]) \
      .map(pe_color, subset=["trailingPE"]) \
      .map(pb_color, subset=["pb_ratio"]) \
      .map(div_color, subset=["dividendYield"]) \
      .map(profit_color, subset=["profitMargins"]) \
      .set_properties(**{"font-family": "Arial, sans-serif", "font-size": "12pt"})

    st.dataframe(styled, use_container_width=True)




# Streamlit tabs
tabs = st.tabs([
    "Holdings",
    "Revenues & Taxes",
    "Investment Planning",
    "Technical Analysis",
    "News & Sentiment",
    "Transactions & Portfolios",
    "Watchlist",
    "Alerts Manager",
    "⚙️ Settings"
])

# ---------- PORTFOLIOS ----------
with tabs[0]:
    st.header("📊 Holdings")

    # --- Load portfolios & snapshot (aggregated) ---
    p_df = mw.db.list_portfolios() if hasattr(mw, 'db') else mw.list_portfolios()
    all_names = p_df['name'].tolist() if not p_df.empty else []
    id_map = {row['name']: row['id'] for _, row in p_df.iterrows()} if not p_df.empty else {}
    selected_portfolio_ids = list(id_map.values())

    # snapshot aggregated across portfolios: used for filters + table + summary
    holdings_snapshot = mw.get_latest_holdings_snapshot(aggregate=False)

    if holdings_snapshot.empty:
        st.info("No holdings available")
    else:

        with st.expander("Filters", expanded=False):
            # --- Portfolio selection ---
            selected_ports = st.multiselect("Portfolios", all_names, default=None)
            selected_ids = [id_map[n] for n in selected_ports] if selected_ports else None

            # --- Prepare snapshot copy with filled metadata ---
            df_filtered = holdings_snapshot.copy()
            for col in ['sector','industry','security_type','exchange','security_label']:
                df_filtered[col] = df_filtered.get(col, "Unknown").fillna("Unknown")

            # --- Other filters ---
            sel_sec = st.multiselect("Sector", sorted(df_filtered['sector'].unique()), default=None)
            sel_ind = st.multiselect("Industry", sorted(df_filtered['industry'].unique()), default=None)
            sel_type = st.multiselect("Security Type", sorted(df_filtered['security_type'].unique()), default=None)
            sel_exch = st.multiselect("Exchange", sorted(df_filtered['exchange'].unique()), default=None)

        # --- Apply filters (only if selection made) ---
        h_filtered = df_filtered.copy()

        if selected_ids:
            h_filtered = h_filtered[h_filtered['portfolio_id'].isin(selected_ids)]
        if sel_sec:
            h_filtered = h_filtered[h_filtered['sector'].isin(sel_sec)]
        if sel_ind:
            h_filtered = h_filtered[h_filtered['industry'].isin(sel_ind)]
        if sel_type:
            h_filtered = h_filtered[h_filtered['security_type'].isin(sel_type)]
        if sel_exch:
            h_filtered = h_filtered[h_filtered['exchange'].isin(sel_exch)]


        st.subheader("Summary")

        if h_filtered.empty:
            st.warning("No holdings match the selected filters.")
        else:
            # --- Holding Performance expander (summary + charts) ---

            # --- Summary KPIs ---
            summary_df = h_filtered.copy()
            total_cost = summary_df['cost_basis'].sum()
            total_market = summary_df['market_value'].sum()
            total_abs_perf = summary_df['abs_perf'].sum()
            total_rel_perf = (total_market - total_cost) / total_cost if total_cost else 0.0

            # --- Timeseries (needed for indicators) ---
            df_ts = mw.holdings_timeseries(
                portfolio_ids=selected_ids or None,
                sectors=sel_sec or None,
                industries=sel_ind or None,
                security_types=sel_type or None,
                symbols=None,
                exchanges=sel_exch or None
            )

            # Defaults
            mv_status, cost_status, abs_perf_status, rel_perf_status = "", "", "", ""
            vol_status, sharpe_status = "", ""

            if not df_ts.empty:
                df_ts = df_ts.sort_values("date").reset_index(drop=True)
                df_ts['daily_return'] = df_ts['market_value'].pct_change()

                # --- Market Value trend (vs 30d ago) ---
                today = df_ts['date'].max()
                past = today - pd.Timedelta(days=30)
                mv_now = df_ts.loc[df_ts['date'] == today, 'market_value'].values[0]
                mv_past = df_ts.loc[df_ts['date'] >= past, 'market_value'].iloc[0] if any(df_ts['date'] >= past) else mv_now
                mv_change = (mv_now - mv_past) / mv_past if mv_past else 0

                if mv_change > 0.05:
                    mv_status = "📈 Growing"
                elif mv_change < -0.05:
                    mv_status = "📉 Declining"
                else:
                    mv_status = "📊 Flat"

                # --- Total Cost inflow/outflow ---
                cost_now = df_ts.loc[df_ts['date'] == today, 'cost_basis'].values[0]
                cost_past = df_ts.loc[df_ts['date'] >= past, 'cost_basis'].iloc[0] if any(df_ts['date'] >= past) else cost_now
                cost_change = cost_now - cost_past

                if cost_change > 100:
                    cost_status = "💰 Inflow"
                elif cost_change < -100:
                    cost_status = "⬇️ Outflow"
                else:
                    cost_status = "⚠️ Flat"

                # --- Abs Perf vs YTD avg ---
                ytd_start = dt.date(today.year, 1, 1)
                ytd_df = df_ts[df_ts['date'] >= pd.to_datetime(ytd_start)]
                ytd_avg_profit = (ytd_df['market_value'] - ytd_df['cost_basis']).mean() if not ytd_df.empty else 0
                if total_abs_perf > 1.1 * ytd_avg_profit:
                    abs_perf_status = "🌟 Above YTD avg"
                elif total_abs_perf < 0.9 * ytd_avg_profit:
                    abs_perf_status = "⚠️ Below YTD avg"
                else:
                    abs_perf_status = "📊 Near YTD avg"

                # --- Rel Perf vs benchmark (MSCI World ~8%) ---
                benchmark = 0.08
                if total_rel_perf > benchmark + 0.02:
                    rel_perf_status = "🌟 Outperforming"
                elif total_rel_perf < benchmark - 0.02:
                    rel_perf_status = "⚠️ Underperforming"
                else:
                    rel_perf_status = "📊 In line"

                # --- Volatility (std of daily returns * sqrt(252)) ---
                volatility = df_ts['daily_return'].std() * np.sqrt(252)
                if volatility < 0.10:
                    vol_status = "🌟 Low"
                elif volatility < 0.20:
                    vol_status = "📊 Medium"
                else:
                    vol_status = "⚠️ High"

                # --- Sharpe Ratio ---
                avg_return = df_ts['daily_return'].mean() * 252
                sharpe = (avg_return - 0.02) / volatility if volatility and not np.isnan(volatility) else np.nan
                if sharpe > 1.0:
                    sharpe_status = "🌟 Good"
                elif sharpe > 0.0:
                    sharpe_status = "📊 Okay"
                else:
                    sharpe_status = "⚠️ Poor"

            else:
                volatility, sharpe = np.nan, np.nan

            # --- KPI Row ---
            k_cols = st.columns(6)
            k_cols[0].metric("Market Value (€)", f"{total_market:,.2f}", mv_status)
            k_cols[1].metric("Total Cost (€)", f"{total_cost:,.2f}", cost_status)
            k_cols[2].metric("Abs Perf (€)", f"{total_abs_perf:,.2f}", abs_perf_status)
            k_cols[3].metric("Rel Perf (%)", f"{total_rel_perf*100:.2f}%", rel_perf_status)
            k_cols[4].metric("Volatility", f"{volatility:.2%}" if not np.isnan(volatility) else "N/A", vol_status)
            k_cols[5].metric("Sharpe Ratio", f"{sharpe:.2f}" if not np.isnan(sharpe) else "N/A", sharpe_status)

            st.write("")  # small linebreak

            # --- Table: current holdings (snapshot) ---
            table_df = h_filtered.copy()

            # Determine if we should aggregate
            aggregate = not selected_ports or len(selected_ports) > 1  # None or more than 1 selected

            if aggregate:
                # Sum key numeric fields across portfolios
                agg_cols_sum = ['quantity','cost_basis','market_value','abs_perf']
                table_df = table_df.groupby('security_label', as_index=False)[agg_cols_sum].sum()

            # Recompute calculated columns
            table_df['Avg Cost/Share (€)'] = (table_df['cost_basis'] / table_df['quantity']).replace([np.inf, -np.inf, np.nan], 0.0)
            table_df['Current Price (€)'] = (table_df['market_value'] / table_df['quantity']).replace([np.inf, -np.inf, np.nan], 0.0)
            table_df['Total Cost (€)'] = table_df['cost_basis']
            table_df['Market Value (€)'] = table_df['market_value']
            table_df['Abs Performance (€)'] = table_df['abs_perf']
            table_df['Rel Performance (%)'] = table_df['abs_perf'] / table_df['cost_basis'].replace({0: np.nan})

            # --- Search / filter ---
            search = st.multiselect(
                "🔍 Search security",
                options=table_df['security_label'].tolist(),
                default=None
            )
            if search:
                table_df = table_df[table_df['security_label'].isin(search)]

            # --- Display ---
            display_cols = ['security_label','quantity','Avg Cost/Share (€)','Total Cost (€)',
                            'Current Price (€)','Market Value (€)','Abs Performance (€)','Rel Performance (%)']
            display = table_df[display_cols].copy()
            display = display.rename(columns={'security_label':'Security','quantity':'Quantity'})

            # Format and color
            def color_perf(val):
                if pd.isna(val):
                    return ''
                try:
                    v = float(val)
                except Exception:
                    return ''
                return 'color: green' if v > 0 else ('color: red' if v < 0 else '')

            styled = display.style.format({
                'Avg Cost/Share (€)':'{:,.2f}',
                'Total Cost (€)':'{:,.2f}',
                'Current Price (€)':'{:,.2f}',
                'Market Value (€)':'{:,.2f}',
                'Abs Performance (€)':'{:,.2f}',
                'Rel Performance (%)':'{:.2%}'
            }).map(color_perf, subset=['Abs Performance (€)','Rel Performance (%)'])

            st.dataframe(styled, use_container_width=True)



            if df_ts.empty:
                st.info("No historical timeseries data available for the selected filters.")
            else:
                st.subheader("Performance")
            
                with st.expander("Deeper Performance Analysis", expanded=False):
                    # Ensure sorting & numeric
                    df_ts = df_ts.sort_values('date').reset_index(drop=True)
                    df_ts['market_value'] = pd.to_numeric(df_ts['market_value'], errors='coerce').fillna(0.0)
                    df_ts['cost_basis'] = pd.to_numeric(df_ts['cost_basis'], errors='coerce').fillna(0.0)
                    df_ts['net_value'] = df_ts['market_value'] - df_ts['cost_basis']

                    # Aggregate per date (in case multiple securities/portfolios included)
                    df_daily = df_ts.groupby('date')[['market_value','cost_basis','net_value']].sum().reset_index()
                    df_daily = df_daily.sort_values('date')
                    df_daily['cum_market'] = df_daily['market_value'].cumsum()
                    df_daily['cum_net'] = df_daily['net_value'].cumsum()
                    df_daily['cum_rel_perf'] = df_daily['cum_net'] / df_daily['cost_basis'].cumsum().replace({0: np.nan})

                    # Instantaneous relative performance (per date) = net / cost at that date
                    df_daily['rel_perf_inst'] = np.where(df_daily['cost_basis'] != 0, (df_daily['market_value'] - df_daily['cost_basis']) / df_daily['cost_basis'], np.nan)

                    col = st.columns(2)
                    with col[0]:
                        # Chart: market_value and net_value (NOT cumsum) — final value should match snapshot
                        fig_mn = px.area(
                            df_daily,
                            x='date',
                            y=['net_value', 'market_value'],
                            labels={'value': '€', 'variable': 'Value Type', 'date': 'Date'},
                            title="Market Value and Net Value Over Time (market_value vs market_value - cost_basis)"
                        )
                        # colors & visual hints
                        fig_mn.update_traces(selector=dict(name='net_value'), line=dict(color='green'), opacity=0.8)
                        fig_mn.update_traces(selector=dict(name='market_value'), line=dict(color='blue'), opacity=0.6)
                        # show vertical spike on hover to compare
                        fig_mn.update_xaxes(showspikes=True, spikemode='across', spikecolor='grey', spikesnap='cursor')
                        fig_mn.update_layout(hovermode='x unified', yaxis_title="Value (€)")
                        st.plotly_chart(fig_mn, use_container_width=True)
                        st.caption("**Interpretation:** Market value is your portfolio's total quoted value at each date. Net value = market value − cost basis (the realized/unrealized profit). The last datapoint for Market Value should match the snapshot Market Value above.")

                    with col[1]:
                        # --- Chart: cumulative net (bottom) and cumulative market (top) overlapping ---
                        fig1 = px.area(
                            df_daily,
                            x='date',
                            y=['cum_net','cum_market'],
                            labels={'value':'€','variable':'Value Type','date':'Date'},
                            title="Cumulative Net Value (bottom) and Cumulative Market Value (top)"
                        )
                        # Make colors and overlapping appearance clearer by editing traces
                        fig1.update_traces(selector=dict(name='cum_net'), line=dict(color='green'))
                        fig1.update_traces(selector=dict(name='cum_market'), line=dict(color='blue'), opacity=0.6)
                        fig1.update_layout(yaxis_title="Value (€)", xaxis_title="Date", hovermode="x unified")
                        st.plotly_chart(fig1, use_container_width=True)
                        st.caption("Market value vs Net profit. Shows how invested capital and gains evolve over time.")
                    
                    col = st.columns(2)
                    with col[0]:
                        # Chart: Relative Performance over time (instantaneous) — ends at snapshot rel perf
                        fig_rel = px.line(df_daily, x='date', y='rel_perf_inst', title="Relative Performance Over Time")
                        fig_rel.update_layout(hovermode='x unified', yaxis_title="Rel Perf (ratio)")
                        fig_rel.update_xaxes(showspikes=True, spikemode='across', spikecolor='grey', spikesnap='cursor')
                        st.plotly_chart(fig_rel, use_container_width=True)
                        st.caption("**Interpretation:** This shows the portfolio performance relative to the invested cost at each date. The final point equals the snapshot relative performance.")


                    with col[1]:
                        # --- Chart: cumulative relative performance ---
                        fig2 = px.area(df_daily, x='date', y='cum_rel_perf',
                                       labels={'cum_rel_perf':'Cumulative Rel Perf (ratio)'},
                                       title="Cumulative Relative Performance (cum_net / cum_cost)")
                        fig2.update_xaxes(showspikes=True, spikemode='across', spikecolor='grey', spikesnap='cursor')
                        st.plotly_chart(fig2, use_container_width=True)
                        st.caption("Cumulative relative performance. Ratio of profit vs invested cost basis.")

                    # ---------- Risk-Adjusted Performance ----------

                    # cagr_dict = mw.compute_cagr(df_daily)

                    # if not cagr_dict:
                    #     st.info("Not enough data to compute CAGR.")
                    # else:
                    #     col1, col2, col3, col4 = st.columns(4)
                    #     col1.metric("1Y CAGR", f"{cagr_dict['1y']:.2%}" if cagr_dict['1y'] else "N/A")
                    #     col2.metric("3Y CAGR", f"{cagr_dict['3y']:.2%}" if cagr_dict['3y'] else "N/A")
                    #     col3.metric("5Y CAGR", f"{cagr_dict['5y']:.2%}" if cagr_dict['5y'] else "N/A")
                    #     col4.metric("Since Inception", f"{cagr_dict['since_inception']:.2%}" if cagr_dict['since_inception'] else "N/A")

                    # # --- Annual CAGR bar chart ---
                    # df_cagr = mw.add_annual_performance(df_daily[['date','net_value']])
                    # if df_cagr.empty:
                    #     st.info("Not enough data to compute annual CAGR.")
                    # else:
                    #     fig_cagr = px.bar(df_cagr, x='year', y='annual_cagr', text='annual_cagr',
                    #                       title="Annual Compound Growth Rate (CAGR)")
                    #     fig_cagr.update_traces(texttemplate='%{text:.2%}')
                    #     st.plotly_chart(fig_cagr, use_container_width=True)
                    #     st.caption("Annual CAGR shown by calendar year. For short time spans (<3 years), interpret with caution.")

                    # # --- Rolling CAGR / Rolling Returns ---
                    # cagr_window = st.selectbox("Rolling horizon (years)", [1,3,5], index=1)
                    # df_rolling = mw.add_rolling_cagr(df_daily, years=cagr_window)
                    # if df_rolling.empty:
                    #     st.info("Not enough data to compute rolling CAGR.")
                    # else:
                    #     fig_rolling = px.line(df_rolling, x='date', y='cagr', 
                    #                           title=f"Rolling {cagr_window}Y CAGR")
                    #     fig_rolling.update_yaxes(tickformat=".2%")
                    #     st.plotly_chart(fig_rolling, use_container_width=True)
                    #     st.caption(f"Rolling {cagr_window}-year CAGR: helps identify periods of outperformance/underperformance.")

                    # --- Drawdown chart ---
                    df_daily['cum_max'] = df_daily['net_value'].cummax()
                    df_daily['drawdown'] = (df_daily['net_value'] - df_daily['cum_max']) / df_daily['cum_max']
                    fig_dd = px.area(df_daily, x='date', y='drawdown', title="Portfolio Drawdown Over Time")
                    fig_dd.update_yaxes(tickformat=".2%")
                    st.plotly_chart(fig_dd, use_container_width=True)
                    st.caption("Drawdown = percentage drop from previous peak. Highlights max risk exposure during downturns.")

                     # --- Scatterplot / cluster-like grouping (performance groups) ---
                    scatter_df = h_filtered.copy()[['security_label','market_value','abs_perf','rel_perf','quantity','security_type']].copy()
                    scatter_df['Rel Perf (%)'] = scatter_df['rel_perf']
                    scatter_df['Market Value Abs'] = scatter_df['market_value'].abs()

                    # --- Performance buckets ---
                    perf_labels = ['Very Poor','Poor','Neutral','Good','Excellent']
                    scatter_df['performance_group'] = pd.cut(
                        scatter_df['rel_perf'],
                        bins=[-np.inf, -0.10, -0.02, 0.02, 0.10, np.inf],
                        labels=perf_labels
                    )

                    # --- Multi-select for performance groups ---
                    selected_groups = st.multiselect(
                        "Select performance groups to display (leave empty = all)",
                        options=perf_labels,
                        default=[]
                    )

                    if selected_groups:
                        scatter_df = scatter_df[scatter_df['performance_group'].isin(selected_groups)]

                    # --- Remove unused categories ---
                    scatter_df['performance_group'] = scatter_df['performance_group'].cat.remove_unused_categories()

                    if scatter_df.empty:
                        st.info("No securities available for the selected filters.")
                    else:
                        # --- Bubble size ---
                        min_size = 10
                        max_size = 60
                        scatter_df['size_scaled'] = scatter_df['Market Value Abs'].apply(lambda x: max(x, min_size))

                        # --- Colors and category order ---
                        color_map = {
                            'Very Poor':'#d7191c', 'Poor':'#fdae61', 'Neutral':'#ffffbf',
                            'Good':'#a6d96a', 'Excellent':'#1a9641'
                        }
                        present_categories = scatter_df['performance_group'].cat.categories.tolist()

                        # --- Scatter plot ---
                        fig_scatter = px.scatter(
                            scatter_df,
                            x='Market Value Abs',
                            y='abs_perf',
                            color='performance_group',
                            size='size_scaled',
                            size_max=max_size,
                            hover_name='security_label',
                            title='Holdings: exposure vs absolute performance',
                            labels={'Market Value Abs':'Market Value (€)', 'abs_perf':'Abs Performance (€)'},
                            color_discrete_map={k: color_map[k] for k in present_categories},
                            category_orders={'performance_group': present_categories}
                        )
                        fig_scatter.update_layout(legend_title_text="Perf Group")
                        st.plotly_chart(fig_scatter, use_container_width=True)
                        st.caption("**Interpretation:** Colors = performance buckets (relative perf). Bubble size = exposure (abs market value).")

                        # --- Securities overview per group ---
                        st.subheader("Securities per performance group")
                        for grp in present_categories:
                            grp_df = scatter_df[scatter_df['performance_group'] == grp]
                            if grp_df.empty:
                                st.markdown(f"- **{grp}**: None")
                                continue

                            # Sort by market value descending
                            sorted_df = grp_df.sort_values('market_value', ascending=False)
                            names = sorted_df['security_label'].tolist()

                            # Display 10 per line
                            lines = [', '.join(names[i:i+10]) for i in range(0, len(names), 10)]
                            display_text = "<br>".join(lines)

                            # Show hidden count if >50
                            if len(names) > 50:
                                display_text += f" <br> (+{len(names)-50} more)"

                            st.markdown(f"- **{grp}**: {display_text}", unsafe_allow_html=True)


                # ---------- Composition (pie / treemap) ----------
                st.subheader("Holdings Composition Analysis")
                with st.expander("Portfolio Composition Charts", expanded=False):
                    exclude_unknown = st.checkbox("Exclude 'Unknown' values from composition charts", value=True)
                    categories = {'security_type': 'pie', 'sector': 'treemap', 'industry': 'treemap', 'exchange': 'treemap'}
                    charts = []
                    for col, kind in categories.items():
                        if col not in h_filtered.columns:
                            continue
                        dfc = h_filtered.copy()
                        dfc[col] = dfc[col].fillna('Unknown')
                        if exclude_unknown:
                            dfc = dfc[dfc[col] != 'Unknown']
                        dfc_grouped = dfc.groupby(col)['market_value'].sum().reset_index().sort_values('market_value', ascending=False)
                        if dfc_grouped.empty:
                            st.info(f"No data for {col}")
                            continue
                        if kind == 'pie':
                            fig = px.pie(dfc_grouped, names=col, values='market_value', title=f"By {col}")
                            fig.update_traces(textinfo='percent+label', hovertemplate='%{label}: €%{value:,.2f} (%{percent})')
                        else:
                            fig = px.treemap(dfc_grouped, path=[col], values='market_value', title=f"By {col}")
                            fig.data[0].texttemplate = "%{label}<br>€%{value:,.2f}"
                        charts.append(fig)

                    # display two per row
                    for i in range(0, len(charts), 2):
                        cols = st.columns(2)
                        for j in range(2):
                            if i + j < len(charts):
                                with cols[j]:
                                    st.plotly_chart(charts[i + j], use_container_width=True)
                                    st.caption("**Interpretation:** Composition charts show how the portfolio's market value is distributed across the respective dimension.")

                st.subheader("Holdings KPIs")
                with st.expander("Holdings KPIs", expanded=False):


                    # --- Enrich holdings with basic info ---
                    enriched_rows = []

                    for sym in h_filtered['symbol'].unique():
                        basics = mw.get_security_basic(sym)  # returns a dict with all additional fields
                        if basics:
                            enriched_rows.append(basics)

                    # Convert to DataFrame
                    df_basics = pd.DataFrame(enriched_rows)

                    # Merge with h_filtered on 'symbol'
                    h_enriched = h_filtered.merge(df_basics, on='symbol', how='left')

                    # Fill missing string values with "N/A"
                    string_cols = ['longName', 'shortName', 'country', 'exchange', 'sector', 'industry']
                    for col in string_cols:
                        if col in h_enriched.columns:
                            h_enriched[col] = h_enriched[col].fillna("N/A")

                    # Ensure 'security_name' column exists
                    if 'longName' in h_enriched.columns:
                        h_enriched['security_name'] = h_enriched['longName']
                    elif 'shortName' in h_enriched.columns:
                        h_enriched['security_name'] = h_enriched['shortName']
                    else:
                        h_enriched['security_name'] = "N/A"

                    # Ensure numeric columns are floats
                    numeric_cols = [
                        'regularMarketPrice', 'fiftyTwoWeekHigh', 'fiftyTwoWeekLow',
                        'volume', 'averageVolume', 'marketCap', 'beta',
                        'trailingPE', 'forwardPE', 'trailingEps', 'dividendRate', 
                        'dividendYield', 'enterpriseValue', 'profitMargins',
                        'operatingMargins', 'returnOnAssets', 'returnOnEquity',
                        'totalRevenue', 'revenuePerShare', 'grossProfits', 'ebitda',
                        'totalCash', 'totalDebt', 'currentRatio', 'bookValue',
                        'operatingCashflow', 'freeCashflow', 'sharesOutstanding'
                    ]
                    for col in numeric_cols:
                        if col in h_enriched.columns:
                            h_enriched[col] = pd.to_numeric(h_enriched[col], errors='coerce')

                    # Compute P/B ratio if possible
                    if 'regularMarketPrice' in h_enriched.columns and 'bookValue' in h_enriched.columns:
                        h_enriched['pb_ratio'] = h_enriched['regularMarketPrice'] / h_enriched['bookValue']
                    else:
                        h_enriched['pb_ratio'] = None

                    # Now h_enriched is ready for display_kpi_table
                    display_kpi_table(h_enriched)



# ---------- REVENUES & TAXES ----------
with tabs[1]:
    st.header("Revenues & Taxes")

    # --- Load portfolios ---
    p_df = db.list_portfolios()
    all_portfolios = p_df['name'].tolist() if not p_df.empty else []
    pid_to_name = {r['id']: r['name'] for _, r in p_df.iterrows()} if not p_df.empty else {}

    # --- Filters (collapsed by default) ---
    with st.expander("Filters", expanded=False):
        sel_ports = st.multiselect("Select portfolios", options=all_portfolios, default=[], key="tax_port_sel")
        port_ids = [int(p_df[p_df['name']==n]['id'].iloc[0]) for n in sel_ports] if sel_ports else None

        # Load data
        cgs_all = mw.calc_capital_gains_fifo(portfolio_ids=port_ids)
        divs_all = mw.calc_dividends_for_portfolio(portfolio_ids=port_ids)

        # Securities
        sec_from_cg = cgs_all['symbol'].unique() if not cgs_all.empty else []
        sec_from_divs = divs_all['symbol'].unique() if not divs_all.empty else []
        all_syms = sorted(set(sec_from_cg) | set(sec_from_divs))
        sel_syms = st.multiselect(
            "Select securities", options=all_syms, default=[], format_func=format_sec_label, key="tax_sec_sel"
        )

        # Years
        years_cg = cgs_all['year'].unique() if not cgs_all.empty else []
        years_div = divs_all['year'].unique() if not divs_all.empty else []
        all_years = sorted(set(years_cg) | set(years_div))
        sel_years = st.multiselect("Select years", options=all_years, default=[], key="tax_year_sel")

        # Type filter
        sel_type = st.multiselect(
            "Select type", options=["Capital Gains", "Dividends"], default=[], key="tax_type_sel"
        )

    # --- Apply filters ---
    cgs = cgs_all.copy() if not cgs_all.empty else pd.DataFrame()
    divs = divs_all.copy() if not divs_all.empty else pd.DataFrame()

    if sel_syms:
        cgs = cgs[cgs['symbol'].isin(sel_syms)] if not cgs.empty else cgs
        divs = divs[divs['symbol'].isin(sel_syms)] if not divs.empty else divs
    if sel_years:
        cgs = cgs[cgs['year'].isin(sel_years)] if not cgs.empty else cgs
        divs = divs[divs['year'].isin(sel_years)] if not divs.empty else divs
    if sel_type:
        if "Capital Gains" not in sel_type:
            cgs = pd.DataFrame()
        if "Dividends" not in sel_type:
            divs = pd.DataFrame()

    # --- Totals & Estimated Taxes (no arrows) ---
    total_gains = cgs['profit'].sum() if not cgs.empty else 0.0
    total_divs = divs['total'].sum() if not divs.empty else 0.0
    tax_rate = get_config("tax_rate")
    tax_gains = total_gains * tax_rate
    tax_divs = total_divs * tax_rate
    total_tax = tax_gains + tax_divs

    # --- Totals & Estimated Taxes (table version) ---
    if not cgs.empty or not divs.empty:
        summary_df = pd.DataFrame({
            "Type": ["Capital Gains", "Dividends", "Total"],
            "Amount (€)": [
                total_gains, total_divs, total_gains + total_divs
            ],
            "Tax Due (€)": [
                tax_gains, tax_divs, total_tax
            ]
        })

        # Format values nicely
        summary_df["Amount (€)"] = summary_df["Amount (€)"].map(lambda x: f"€{x:,.2f}")
        summary_df["Tax Due (€)"] = summary_df["Tax Due (€)"].map(lambda x: f"€{x:,.2f}")

        st.subheader("Totals & Estimated Taxes")
        st.dataframe(summary_df, height=120)

    # --- Combine data ---
    combined_list = []

    if not cgs.empty:
        cgs = cgs.copy()
        cgs['Type'] = "Capital Gains"
        cgs = cgs.rename(columns={"profit":"Amount"})
        combined_list.append(cgs[['portfolio_id','symbol','year','Amount','Type']])

    if not divs.empty:
        divs = divs.copy()
        divs['Type'] = "Dividends"
        divs = divs.rename(columns={"total":"Amount"})
        combined_list.append(divs[['portfolio_id','symbol','year','Amount','Type']])

    if combined_list:
        combined = pd.concat(combined_list, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=['portfolio_id','symbol','year','Amount','Type'])


    if combined.empty:
        st.info("No data for selected filters.")
    else:
        combined['portfolio'] = combined['portfolio_id'].apply(lambda x: pid_to_name.get(int(x), str(x)))
        combined['Security Label'] = combined['symbol'].apply(format_sec_label)

        # --- 1. By Year (stacked) ---
        fig_year = px.bar(
            combined.groupby(['year','Type'])['Amount'].sum().reset_index().sort_values('year'),
            x='year', y='Amount', color='Type', text='Amount',
            labels={'Amount':'Amount (€)', 'year':'Year'},
            color_discrete_map={'Capital Gains':'#636EFA','Dividends':'#00CC96'},
            barmode='stack'
        )
        fig_year.update_traces(texttemplate='€%{text:,.2f}', textposition='outside')
        st.subheader("By Year")
        st.plotly_chart(fig_year, use_container_width=True)

        # --- 2. By Security (horizontal, stacked, scrollable) ---
        agg_sec = combined.groupby(['Security Label','Type'])['Amount'].sum().reset_index()
        agg_sec = agg_sec.sort_values('Amount', ascending=True)
        chart_height = max(400, 40 * len(agg_sec['Security Label'].unique()))

        fig_sec = px.bar(
            agg_sec,
            x='Amount',
            y='Security Label',
            orientation='h',
            color='Type',
            text='Amount',
            labels={'Amount':'Amount (€)','Security Label':'Security'},
            color_discrete_map={'Capital Gains':'#636EFA','Dividends':'#00CC96'},
            barmode='stack'
        )
        fig_sec.update_traces(texttemplate='€%{text:,.2f}', textposition='inside')
        fig_sec.update_yaxes(tickangle=0, automargin=True)
        fig_sec.update_layout(height=chart_height, margin=dict(l=200, r=50, t=50, b=50))
        st.subheader("By Security")
        st.plotly_chart(fig_sec, use_container_width=True)

        # --- 3. By Portfolio (stacked, sorted) ---
        agg_port = combined.groupby(['portfolio','Type'])['Amount'].sum().reset_index()
        agg_port = agg_port.sort_values('Amount', ascending=True)
        fig_port = px.bar(
            agg_port, x='portfolio', y='Amount', color='Type', text='Amount',
            labels={'Amount':'Amount (€)','portfolio':'Portfolio'},
            color_discrete_map={'Capital Gains':'#636EFA','Dividends':'#00CC96'},
            barmode='stack'
        )
        fig_port.update_traces(texttemplate='€%{text:,.2f}', textposition='inside')
        st.subheader("By Portfolio")
        st.plotly_chart(fig_port, use_container_width=True)

        # --- Negative Capital Gains ---
        if not cgs.empty:
            neg_cgs = cgs[cgs['Amount'] < 0].copy()
            if not neg_cgs.empty:
                neg_cgs['portfolio'] = neg_cgs['portfolio_id'].apply(lambda x: pid_to_name.get(int(x), str(x)))
                neg_cgs['Security Label'] = neg_cgs['symbol'].apply(format_sec_label)
                
                st.subheader("Negative Capital Gains (Potential Tax-Loss Opportunities)")
                st.dataframe(
                    neg_cgs[['Security Label','Amount','year','portfolio']]
                    .rename(columns={'Amount':'Amount (€)','year':'Year','portfolio':'Portfolio'}),
                    height=300
                )

# ------------ INVESTMENT PLANNING
with tabs[2]:
    st.header("🧭 Investment Planning")

    # --- Portfolio filter (unique key to avoid clashes with Holdings tab) ---
    p_df = mw.db.list_portfolios() if hasattr(mw, 'db') else mw.list_portfolios()
    all_names = p_df['name'].tolist() if not p_df.empty else []
    id_map = {row['name']: row['id'] for _, row in p_df.iterrows()} if not p_df.empty else {}
    selected_ports = st.multiselect("Portfolios", all_names, default=None, key="planning_ports")
    selected_ids = [id_map[n] for n in selected_ports] if selected_ports else list(id_map.values())

    holdings_snapshot = mw.get_latest_holdings_snapshot(aggregate=False)
    if holdings_snapshot.empty:
        st.info("No holdings available for planning.")
    else:
        # --- 1. Filter by selected portfolios ---
        df_filtered = holdings_snapshot[holdings_snapshot['portfolio_id'].isin(selected_ids)].copy()

        # --- 2. Current vs. Past Holdings Toggle ---
        st.markdown("#### Current vs. Past Holdings")
        show_current_only = st.checkbox("Show only currently held securities", value=True)
        if show_current_only:
            # Get latest snapshot for selected portfolios
            latest_snapshot = mw.get_latest_holdings_snapshot(portfolio_ids=selected_ids, aggregate=False)
            if not latest_snapshot.empty:
                current_secs = latest_snapshot["symbol"].unique()
                df_filtered = df_filtered[df_filtered["symbol"].isin(current_secs)]

        # --- 3. Security Type filter ---
        available_types = sorted(df_filtered['security_type'].dropna().unique().tolist())
        selected_types = st.multiselect(
            "Security Types", 
            options=available_types, 
            default=None,  # preselect all
            key="planning_sec_types"
        )

        if selected_types:
            df_filtered = df_filtered[df_filtered['security_type'].isin(selected_types)]


        # --- Load saved configs safely ---
        sectors_config    = safe_json_load(get_config("target_sector_allocation"), {})
        industries_config = safe_json_load(get_config("target_industry_allocation"), {})
        asset_targets_raw = safe_json_load(get_config("asset_allocation_targets"), {})
        # Map old keys to new keys if needed
        asset_targets = {
            "pre_retirement": asset_targets_raw.get("pre_retirement") or asset_targets_raw.get("current") or {},
            "post_retirement": asset_targets_raw.get("post_retirement") or asset_targets_raw.get("retirement") or {}
        }
        risk_cfg          = safe_json_load(get_config("target_risk_profile"), {})
        retirement   = get_config("retirement_year") or 2047

        with st.expander("Settings"):

            # --- Retirement Year ---
            st.markdown("#### Retirement Year")
            retirement_year = st.number_input(
                "Retirement Year", 
                value=int(retirement), 
                step=1,
                key="retirement_year"
            )

            # --- Asset Allocation ---
            st.markdown("#### Asset Allocation")
            col_pre, col_post = st.columns(2)
            asset_types = ["Equity", "ETF", "Bonds"]
            asset_pre, asset_post = {}, {}

            for asset in asset_types:
                asset_pre[asset] = col_pre.slider(
                    f"{asset} Pre-Retirement", 0.0, 1.0,
                    value=float(asset_targets["pre_retirement"].get(asset, 0.0)),
                    step=0.05, key=f"alloc_pre_{asset}"
                )
                asset_post[asset] = col_post.slider(
                    f"{asset} Post-Retirement", 0.0, 1.0,
                    value=float(asset_targets["post_retirement"].get(asset, 0.0)),
                    step=0.05, key=f"alloc_post_{asset}"
                )

            def normalize(d):
                s = sum(d.values()) or 1.0
                return {k: v/s for k,v in d.items()}

            asset_pre = normalize(asset_pre)
            asset_post = normalize(asset_post)

            # --- Risk Settings ---
            st.markdown("#### Risk Settings")
            col1, col2, col3 = st.columns(3)
            pre_ret_risk = col1.slider(
                "Target Risk Pre-Retirement (Vol.)", 
                0.0, 1.0, 
                value=float(risk_cfg.get("pre_retirement_risk", 0.4)), 
                step=0.01, key="risk_pre"
            )
            post_ret_risk = col2.slider(
                "Target Risk Post-Retirement (Vol.)", 
                0.0, 1.0, 
                value=float(risk_cfg.get("post_retirement_risk", 0.2)), 
                step=0.01, key="risk_post"
            )
            market_volatility = col3.slider(
                "Market Volatility", 
                0.0, 1.0, 
                value=float(risk_cfg.get("market_volatility", 0.15)), 
                step=0.01, key="market_vol"
            )


            # --- Sector / Industry / Security multiselects (just to control weight inputs) ---
            st.markdown("#### Select Levels for Weight Input (optional)")

            # Build full taxonomy (baseline + holdings)
            taxonomy = mw.get_complete_taxonomy(df_filtered)
            all_sectors = sorted(taxonomy.keys())

            # Ensure defaults only include valid options
            valid_sector_defaults = [s for s, w in sectors_config.items() if w > 0 and s in all_sectors]

            selected_sectors = st.multiselect(
                "Select Sectors", 
                options=all_sectors, 
                default=valid_sector_defaults,
                key="planning_sectors"
            )

            if selected_sectors:
                # Build industry options across all selected sectors
                all_industries = sorted({
                    ind for sec in selected_sectors for ind in taxonomy.get(sec, [])
                })
                selected_industries = st.multiselect(
                    "Select Industries", 
                    options=all_industries, 
                    default=[], 
                    key="planning_industries"
                )
            else:
                selected_industries = st.multiselect(
                    "Select Industries", 
                    options=[], 
                    disabled=True,
                    key="planning_industries"
                )


            # Securities multiselect → only enabled if industries selected
            if selected_industries:
                sec_mask = df_filtered['sector'].isin(selected_sectors) if selected_sectors else pd.Series([True] * len(df_filtered))
                ind_mask = df_filtered['industry'].isin(selected_industries)
                filtered_secs = df_filtered[sec_mask & ind_mask]['symbol'].unique()

                selected_securities = st.multiselect(
                    "Select Securities", 
                    options=filtered_secs,
                    default=[], 
                    format_func=format_sec_label,
                    key="planning_secs"
                )
            else:
                st.multiselect(
                    "Select Securities", 
                    options=[],  # empty
                    disabled=True,
                    key="planning_secs_disabled"
                )
                selected_securities = []

            # --- Weight inputs for selected items ---
            if selected_sectors or selected_industries or selected_securities:
                st.markdown("#### Set Weights")

                sector_weights, industry_weights, security_weights = {}, {}, {}


                for sec in selected_sectors:
                    sector_val = st.number_input(f"Sector: {sec}", 0.0, 1.0,
                                                 value=float(sectors_config.get(sec,0)), step=0.05, key=f"sec_weight_{sec}")
                    sector_weights[sec] = sector_val

                    if sector_val > 0:  # only show children if parent > 0
                        inds = sorted(df_filtered[df_filtered['sector']==sec]['industry'].dropna().unique())
                        if inds:
                            industry_weights[sec] = {}
                            for ind in inds:
                                col1, col2 = st.columns([1, 9])  # offset for indentation
                                with col2:
                                    ind_val = st.number_input(f"{sec} → {ind}", 0.0, 1.0,
                                                              value=float(industries_config.get(sec, {}).get(ind,0)),
                                                              step=0.05, key=f"ind_weight_{sec}_{ind}")
                                if ind_val > 0:
                                    industry_weights[sec][ind] = ind_val

                                    # Securities under this industry
                                    syms = df_filtered[(df_filtered['sector']==sec) & (df_filtered['industry']==ind)]['symbol'].unique()
                                    security_weights_ind = {}
                                    for sym in syms:
                                        col1, col2, col3 = st.columns([2, 8, 0.5])  # deeper indentation
                                        with col2:
                                            val = st.number_input(f"{format_sec_label(sym)}", 0.0, 1.0, value=0.0, step=0.05, key=f"sec_weight_{sym}")
                                        if val > 0:
                                            security_weights_ind[sym] = val
                                    # Normalize securities
                                    total_sec = sum(security_weights_ind.values()) or 1.0
                                    for k,v in security_weights_ind.items():
                                        security_weights_ind[k] = v/total_sec
                                    security_weights.update(security_weights_ind)

                # Normalize industry weights within sector
                for sec, inds in industry_weights.items():
                    total_ind = sum(inds.values()) or 1.0
                    for k,v in inds.items():
                        inds[k] = v/total_ind

            # --- Save / Restore buttons ---
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                save_pressed = st.button("💾 Save Weight Settings", key="save_planning")
            with col_btn2:
                restore_pressed = st.button("🔄 Restore Saved Settings", key="restore_planning")
                if restore_pressed:
                    st.rerun()

            # Persist if save clicked
            if save_pressed:
                # Save sector/industry allocations
                set_config("target_sector_allocation", sectors_config)
                set_config("target_industry_allocation", industries_config)

                # Save updated asset allocation targets (pre/post retirement)
                set_config("asset_allocation_targets", {
                    "pre_retirement": asset_pre,
                    "post_retirement": asset_post
                })

                # Save updated target risk profile (pre/post retirement)
                set_config("target_risk_profile", {
                    "pre_retirement_risk": pre_ret_risk,
                    "post_retirement_risk": post_ret_risk,
                    "market_volatility": market_volatility
                })

                # Save retirement year
                set_config("retirement_year", retirement_year)

                st.success("Settings saved.")

    # --- Charts: always filtered by portfolio selection AND "current holdings only" toggle ---
    with st.expander("Portfolio Analytics Over Time"):
        if holdings_snapshot.empty:
            st.info("No holdings available.")
        else:
            st.subheader("Portfolio Analytics Over Time")

            portfolio_ids = selected_ids if selected_ports else None

            # Get the symbols to include (current holdings only if checkbox selected)
            symbols_to_include = df_filtered['symbol'].unique() if show_current_only else None

            # Fetch holdings timeseries
            df_timeseries = mw.holdings_timeseries(portfolio_ids=portfolio_ids, aggregate=False)

            # Filter timeseries to selected symbols (if applicable)
            if symbols_to_include is not None and len(symbols_to_include) > 0:
                df_timeseries = df_timeseries[df_timeseries['symbol'].isin(symbols_to_include)]

            if not df_timeseries.empty:
                # --- Asset Allocation Over Time (by security_type) ---
                type_map = {"EQUITY": "Equity", "ETF": "ETF", "BONDS": "Bonds", "CRYPTOCURRENCY": "Crypto"}
                df_timeseries['security_type'] = df_timeseries['security_type'].map(type_map).fillna("Other")

                df_grouped = df_timeseries.pivot_table(
                    index='date', columns='security_type', values='market_value', aggfunc='sum'
                ).fillna(0.0)

                # ensure all expected asset types exist
                asset_types = ["Equity", "ETF", "Bonds"]
                for t in asset_types:
                    if t not in df_grouped.columns:
                        df_grouped[t] = 0.0
                df_grouped = df_grouped[asset_types].reset_index()

                # --- Area Chart: Allocation Over Time ---
                if not df_grouped.empty:
                    fig_alloc = px.area(
                        df_grouped, x='date', y=asset_types,
                        title="Portfolio Allocation Over Time (by Asset Type)",
                        groupnorm="fraction"
                    )
                    st.plotly_chart(fig_alloc, use_container_width=True)

                # --- Asset Allocation Comparison (Actual vs Pre/Post Targets) ---
                df_alloc = df_timeseries.groupby('security_type')['market_value'].sum()
                if df_alloc.sum() > 0:
                    df_alloc = df_alloc / df_alloc.sum()
                    df_alloc = df_alloc.to_dict()
                else:
                    df_alloc = {}

                # Pre- and post-retirement targets
                target_pre = asset_targets.get("pre_retirement", {}) if asset_targets else {}
                target_post = asset_targets.get("post_retirement", {}) if asset_targets else {}
                all_assets = sorted(set(df_alloc.keys()) | set(target_pre.keys()) | set(target_post.keys()))

                compare_rows = []
                for a in all_assets:
                    actual = df_alloc.get(a, 0.0)
                    compare_rows.append({"Category": a, "Type": "Actual", "Weight": actual, "Delta": 0.0})
                    if a in target_pre:
                        delta = actual - target_pre[a]
                        compare_rows.append({"Category": a, "Type": "Target (Pre)", "Weight": target_pre[a], "Delta": delta})
                    if a in target_post:
                        delta = actual - target_post[a]
                        compare_rows.append({"Category": a, "Type": "Target (Post)", "Weight": target_post[a], "Delta": delta})

                df_compare_asset = pd.DataFrame(compare_rows)

                if not df_compare_asset.empty:
                    fig_alloc_compare = px.bar(
                        df_compare_asset, x="Category", y="Weight", color="Type",
                        barmode="group", title="Asset Allocation: Actual vs Targets",
                        text=df_compare_asset.apply(lambda row: f"{row['Delta']*100:+.1f}%" if row['Type'] != "Actual" else "", axis=1)
                    )
                    fig_alloc_compare.update_traces(textposition='outside')
                    fig_alloc_compare.update_layout(xaxis_title="Asset Type", yaxis_title="Weight")
                    st.plotly_chart(fig_alloc_compare, use_container_width=True)

                # --- Sector Distribution Over Time ---
                df_sector = df_timeseries.pivot_table(
                    index='date', columns='sector', values='market_value', aggfunc='sum'
                ).fillna(0.0).reset_index()

                if len(df_sector.columns) > 1:
                    fig_sector = px.area(
                        df_sector, x='date', y=df_sector.columns[1:],
                        title="Sector Allocation Over Time", groupnorm="fraction"
                    )
                    st.plotly_chart(fig_sector, use_container_width=True)

                    # --- Sector Comparison ---
                    df_sector_alloc = df_timeseries.groupby("sector")["market_value"].sum()
                    if df_sector_alloc.sum() > 0:
                        df_sector_alloc = (df_sector_alloc / df_sector_alloc.sum()).to_dict()
                    else:
                        df_sector_alloc = {}

                    target_sector_alloc = sectors_config or {}
                    all_sectors = sorted(set(df_sector_alloc.keys()) | set(target_sector_alloc.keys()))

                    compare_rows, delta_rows = [], []
                    for s in all_sectors:
                        actual = df_sector_alloc.get(s, 0.0)
                        compare_rows.append({"Category": s, "Type": "Actual", "Weight": actual})
                        if s in target_sector_alloc:
                            compare_rows.append({"Category": s, "Type": "Target", "Weight": target_sector_alloc[s]})
                            delta_rows.append({"Category": s, "Delta (%)": (actual - target_sector_alloc[s]) * 100})

                    df_compare_sector = pd.DataFrame(compare_rows)
                    if not df_compare_sector.empty:
                        fig_sector_compare = px.bar(
                            df_compare_sector, x="Category", y="Weight", color="Type",
                            barmode="group", title="Sector Allocation: Actual vs Target"
                        )
                        for row in delta_rows:
                            fig_sector_compare.add_annotation(
                                x=row["Category"],
                                y=df_compare_sector[df_compare_sector["Category"] == row["Category"]]["Weight"].max(),
                                text=f"{row['Delta (%)']:+.1f}%",
                                showarrow=False, yshift=10
                            )
                        fig_sector_compare.update_layout(xaxis_title="Sector", yaxis_title="Weight")
                        st.plotly_chart(fig_sector_compare, use_container_width=True)

                # --- Industry Distribution Over Time ---
                df_ind = df_timeseries.pivot_table(
                    index='date', columns='industry', values='market_value', aggfunc='sum'
                ).fillna(0.0).reset_index()

                if len(df_ind.columns) > 1:
                    fig_ind = px.area(
                        df_ind, x='date', y=df_ind.columns[1:],
                        title="Industry Allocation Over Time", groupnorm="fraction"
                    )
                    st.plotly_chart(fig_ind, use_container_width=True)

                    # --- Industry Comparison ---
                    df_ind_alloc = df_timeseries.groupby("industry")["market_value"].sum()
                    if df_ind_alloc.sum() > 0:
                        df_ind_alloc = (df_ind_alloc / df_ind_alloc.sum()).to_dict()
                    else:
                        df_ind_alloc = {}

                    target_industry_alloc = industries_config or {}
                    all_inds = sorted(set(df_ind_alloc.keys()) | set(target_industry_alloc.keys()))

                    compare_rows, delta_rows = [], []
                    for i in all_inds:
                        actual = df_ind_alloc.get(i, 0.0)
                        compare_rows.append({"Category": i, "Type": "Actual", "Weight": actual})
                        if i in target_industry_alloc:
                            target = target_industry_alloc[i]
                            compare_rows.append({"Category": i, "Type": "Target", "Weight": target})
                            delta_rows.append({"Category": i, "Delta (%)": (actual - target) * 100})

                    df_compare_ind = pd.DataFrame(compare_rows)
                    if not df_compare_ind.empty:
                        fig_ind_compare = px.bar(
                            df_compare_ind, x="Category", y="Weight", color="Type",
                            barmode="group", title="Industry Allocation: Actual vs Target"
                        )
                        for row in delta_rows:
                            fig_ind_compare.add_annotation(
                                x=row["Category"],
                                y=df_compare_ind[df_compare_ind["Category"] == row["Category"]]["Weight"].max(),
                                text=f"{row['Delta (%)']:+.1f}%",
                                showarrow=False, yshift=10
                            )
                        fig_ind_compare.update_layout(xaxis_title="Industry", yaxis_title="Weight")
                        st.plotly_chart(fig_ind_compare, use_container_width=True)

                # --- Top 10 Securities Over Time ---
                top_secs = (
                    df_timeseries.groupby('symbol')['market_value']
                    .sum()
                    .sort_values(ascending=False)
                    .head(10)
                    .index
                    .tolist()
                )
                df_top = df_timeseries[df_timeseries['symbol'].isin(top_secs)]
                df_top_grouped = df_top.pivot_table(
                    index='date', columns='symbol', values='market_value', aggfunc='sum'
                ).fillna(0.0).reset_index()

                label_map = {col: format_sec_label(col) for col in df_top_grouped.columns if col != 'date'}
                df_top_grouped = df_top_grouped.rename(columns=label_map)

                if len(df_top_grouped.columns) > 1:
                    fig_top = px.area(
                        df_top_grouped, x='date', y=df_top_grouped.columns[1:],
                        title="Top 10 Securities Over Time", groupnorm="fraction"
                    )
                    fig_top.update_layout(legend_title="Security", legend=dict(itemsizing='constant'))
                    st.plotly_chart(fig_top, use_container_width=True)

                    # --- Top 10 Securities Comparison (Actual only) with labels ---
                    df_sec_alloc = df_top.groupby("symbol")["market_value"].sum()
                    if df_sec_alloc.sum() > 0:
                        df_sec_alloc = (df_sec_alloc / df_sec_alloc.sum()).reset_index()
                        df_sec_alloc.columns = ["Category", "Weight"]
                        df_sec_alloc["Category"] = df_sec_alloc["Category"].apply(format_sec_label)

                        fig_sec_compare = px.bar(
                            df_sec_alloc, x="Category", y="Weight", color="Category",
                            title="Top 10 Securities Allocation (Actual)"
                        )
                        fig_sec_compare.update_traces(
                            text=df_sec_alloc["Weight"].apply(lambda x: f"{x:.1%}"),
                            textposition="outside"
                        )
                        fig_sec_compare.update_layout(
                            showlegend=False,
                            xaxis_title="Security", yaxis_title="Weight",
                            xaxis=dict(showticklabels=True)
                        )
                        st.plotly_chart(fig_sec_compare, use_container_width=True)

    with st.expander("Risk Evolution vs Targets"):
    
        # --- Risk Evolution vs Targets ---
        st.subheader("Risk Analytics Overview")

        # Load target risk profile safely
        risk_cfg = safe_json_load(get_config("target_risk_profile"), {})
        pre_ret_risk = float(risk_cfg.get("pre_retirement_risk", 0.4))
        post_ret_risk = float(risk_cfg.get("post_retirement_risk", 0.2))

        # Fetch portfolio risk
        df_risk_time = mw.fetch_portfolio_risk_timeseries(portfolio_ids=selected_ids, aggregate=False)

        if df_risk_time.empty:
            st.info("No risk data available for the selected portfolios.")
        else:
            df_risk_time["date"] = pd.to_datetime(df_risk_time["date"])

            # --- APPLY CURRENT HOLDINGS ONLY FILTER ---
            if show_current_only:
                # Use the df_filtered from the holdings snapshot (already filtered by portfolios, types, etc.)
                current_symbols = df_filtered["symbol"].unique()
                df_risk_time = df_risk_time[df_risk_time["symbol"].isin(current_symbols)]

            retirement_year_local = retirement_year or 2047

            # Targets
            n_days = max((df_risk_time["date"].max() - df_risk_time["date"].min()).days, 1)
            df_risk_time["Target (Pre)"] = np.where(
                df_risk_time["date"].dt.year <= retirement_year_local,
                pre_ret_risk + (post_ret_risk - pre_ret_risk) * ((df_risk_time["date"] - df_risk_time["date"].min()).dt.days / n_days),
                np.nan
            )
            df_risk_time["Target (Post)"] = np.where(
                df_risk_time["date"].dt.year > retirement_year_local,
                post_ret_risk,
                np.nan
            )

            # --- 1. Portfolio Risk vs Targets ---
            df_risk_agg = df_risk_time.groupby("date")["weighted_risk"].mean().reset_index()
            first_date = df_risk_agg["date"].min()
            years_to_retirement = max(retirement_year_local - first_date.year, 1)
            progress = ((df_risk_agg["date"].dt.year - first_date.year) / years_to_retirement).clip(0, 1)

            df_risk_agg["Target (Pre)"] = np.where(
                df_risk_agg["date"].dt.year <= retirement_year_local,
                pre_ret_risk + (post_ret_risk - pre_ret_risk) * progress,
                np.nan
            )
            df_risk_agg["Target (Post)"] = np.where(
                df_risk_agg["date"].dt.year > retirement_year_local,
                post_ret_risk,
                np.nan
            )

            fig_risk = px.line(
                df_risk_agg, x="date", y="weighted_risk",
                title="Portfolio Risk vs Target",
                labels={"weighted_risk": "Actual Risk"}
            )
            fig_risk.add_scatter(
                x=df_risk_agg["date"], y=df_risk_agg["Target (Pre)"],
                mode="lines", name="Target Risk (Pre-Retirement)",
                line=dict(dash="dash", color="green")
            )
            fig_risk.add_scatter(
                x=df_risk_agg["date"], y=df_risk_agg["Target (Post)"],
                mode="lines", name="Target Risk (Post-Retirement)",
                line=dict(dash="dot", color="red")
            )
            st.plotly_chart(fig_risk, use_container_width=True)

            # --- 2. Risk by Asset Type ---
            if "security_type" in df_risk_time.columns:
                df_type = df_risk_time.pivot_table(index="date", columns="security_type", values="weighted_risk", aggfunc="sum").fillna(0.0)
                fig_type = px.area(df_type.reset_index(), x="date", y=df_type.columns, title="Risk by Asset Type Over Time", groupnorm="fraction")
                st.plotly_chart(fig_type, use_container_width=True)

            # --- 3. Risk by Sector ---
            if "sector" in df_risk_time.columns and df_risk_time["sector"].notna().any():
                df_sector = df_risk_time.pivot_table(index="date", columns="sector", values="weighted_risk", aggfunc="sum").fillna(0.0)
                fig_sector = px.area(df_sector.reset_index(), x="date", y=df_sector.columns, title="Risk by Sector Over Time", groupnorm="fraction")
                st.plotly_chart(fig_sector, use_container_width=True)

            # --- 4. Risk by Industry ---
            if "industry" in df_risk_time.columns and df_risk_time["industry"].notna().any():
                df_ind = df_risk_time.pivot_table(index="date", columns="industry", values="weighted_risk", aggfunc="sum").fillna(0.0)
                fig_ind = px.area(df_ind.reset_index(), x="date", y=df_ind.columns, title="Risk by Industry Over Time", groupnorm="fraction")
                st.plotly_chart(fig_ind, use_container_width=True)

            # --- 5. Top Securities by Risk Contribution ---
            # top_secs = df_risk_time.groupby("symbol")["weighted_risk"].sum().nlargest(10).index
            # df_top = df_risk_time[df_risk_time["symbol"].isin(top_secs)]
            # df_top_pivot = df_top.pivot_table(index="date", columns="symbol", values="weighted_risk", aggfunc="sum").fillna(0.0)
            # fig_top = px.area(df_top_pivot.reset_index(), x="date", y=df_top_pivot.columns, title="Top 10 Securities Risk Contribution", groupnorm="fraction")
            # st.plotly_chart(fig_top, use_container_width=True)

            # --- 5. Top Securities by Risk Contribution ---
            top_secs = df_risk_time.groupby("symbol")["weighted_risk"].sum().nlargest(10).index
            df_top = df_risk_time[df_risk_time["symbol"].isin(top_secs)]
            df_top_pivot = df_top.pivot_table(index="date", columns="symbol", values="weighted_risk", aggfunc="sum").fillna(0.0)

            # Map symbols to formatted labels
            symbol_labels = {symbol: format_sec_label(symbol) for symbol in df_top_pivot.columns}
            df_top_pivot.rename(columns=symbol_labels, inplace=True)

            fig_top = px.area(
                df_top_pivot.reset_index(), 
                x="date", 
                y=df_top_pivot.columns, 
                title="Top 10 Securities Risk Contribution", 
                groupnorm="fraction"
            )

            st.plotly_chart(fig_top, use_container_width=True)


            # --- 6. Aggregate risk by category ---
            def plot_risk_by_category(df, category_col, title, use_formatter=False):
                df_cat = df.groupby(category_col)["weighted_risk"].sum().reset_index()
                total = df_cat["weighted_risk"].sum()
                df_cat["risk_pct"] = df_cat["weighted_risk"] / total * 100

                if use_formatter:
                    df_cat[category_col] = df_cat[category_col].apply(format_sec_label)

                df_cat = df_cat.sort_values("risk_pct", ascending=False)

                fig = px.bar(
                    df_cat, x=category_col, y="risk_pct", text="risk_pct",
                    title=title, labels={"risk_pct": "Risk (%)"}
                )
                fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                fig.add_hline(
                    y=pre_ret_risk * 100,
                    line_dash="dash", line_color="red",
                    annotation_text="Target Risk",
                    annotation_position="top left"
                )
                fig.update_layout(xaxis_title=category_col, yaxis_title="Risk (%)")
                st.plotly_chart(fig, use_container_width=True)

            plot_risk_by_category(df_risk_time, "security_type", "Risk by Asset Type")
            plot_risk_by_category(df_risk_time, "sector", "Risk by Sector")
            plot_risk_by_category(df_risk_time, "industry", "Risk by Industry")
            plot_risk_by_category(df_risk_time, "symbol", "Risk by Security", use_formatter=True)

            # --- 7. Heatmap ---
            df_heat = df_risk_time.groupby("symbol")["weighted_risk"].mean().reset_index()
            df_heat["symbol"] = df_heat["symbol"].apply(format_sec_label)
            df_heat["delta_pct"] = (df_heat["weighted_risk"] - pre_ret_risk) / pre_ret_risk * 100
            df_heat = df_heat.sort_values("delta_pct", ascending=False)

            fig_heat = px.bar(
                df_heat,
                x="symbol", y="delta_pct",
                color="delta_pct",
                color_continuous_scale="RdYlGn_r",
                title="Security Risk Deviation vs Pre-Retirement Target",
                labels={"delta_pct": "% vs target"},
                height=1000 
            )
            fig_heat.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
            fig_heat.update_layout(xaxis_title="Security", yaxis_title="Deviation (%)")
            st.plotly_chart(fig_heat, use_container_width=True)


    # --- Fetch rebalancing suggestions ---
    retirement_year   = get_config("retirement_year") or 2047
    rebalance = mw.suggest_rebalancing(portfolio_ids=selected_ids, retirement_year=retirement_year)

    if rebalance.get("suggestions"):
        st.subheader("Suggested Rebalancing Actions")
        def display_rebalance_table(rebalance_results):
            if not rebalance_results or "suggestions" not in rebalance_results:
                st.info("No recommendations available.")
                return

            suggestions = rebalance_results["suggestions"]
            df = pd.DataFrame(suggestions)
            if df.empty:
                st.info("No rebalancing actions needed.")
                return

            df["Symbol"] = df["symbol"].apply(format_sec_label)
            df["Reason"] = df["reasons"].apply(lambda x: "; ".join(x))
            df["Action"] = df["pct_change"].apply(lambda x: "Increase" if x > 0 else ("Reduce" if x < 0 else "Hold"))
            df["% Change"] = df["pct_change"] * 100  # keep sign for coloring
            df["€ Change"] = df["market_value_change"]
            df["Priority"] = df["priority_score"]
            df["Impact Allocation"] = df["impact_allocation"]
            df["Impact Risk"] = df["impact_risk"]

            display_cols = ["Symbol", "Action", "% Change", "€ Change", "Reason",
                            "Impact Allocation", "Impact Risk", "Priority"]
            df_display = df[display_cols]

            # --- Styling functions ---
            def color_pct(val):
                if val > 0:
                    return "background-color: #4CAF50; color: white; font-weight: bold"  # green
                elif val < 0:
                    return "background-color: #f44336; color: white; font-weight: bold"  # red
                else:
                    return "background-color: #dcdcdc; color: #222; font-weight: bold"  # gray

            def color_priority(val):
                return "color: #222; font-weight: bold; background-color: #f0f0f0"

            styled = (
                df_display.style
                .format({
                    "% Change": "{:.2f}%",
                    "€ Change": "€{:,.0f}",
                    "Impact Allocation": "{:.2%}",
                    "Impact Risk": "{:.2%}",
                    "Priority": "{:.2f}"
                })
                .map(color_pct, subset=["% Change"])
                # .map(color_priority, subset=["Priority"])
                .set_properties(**{"font-family": "Arial, sans-serif", "font-size": "12pt"})
            )

            df_display = df_display.sort_values(by="Priority", ascending=False)

            st.dataframe(styled, use_container_width=True)

        # def display_rebalance_table(rebalance_results):
        #     if not rebalance_results or "suggestions" not in rebalance_results:
        #         st.info("No recommendations available.")
        #         return

        #     suggestions = rebalance_results["suggestions"]
        #     df = pd.DataFrame(suggestions)
        #     if df.empty:
        #         st.info("No rebalancing actions needed.")
        #         return

        #     df["Symbol"] = df["symbol"].apply(format_sec_label)
        #     df["Reason"] = df["reasons"].apply(lambda x: "; ".join(x))
        #     df["Action"] = df["pct_change"].apply(lambda x: "Increase" if x > 0 else ("Reduce" if x < 0 else "Hold"))
        #     df["% Change"] = df["pct_change"].apply(lambda x: abs(x)*100)
        #     df["€ Change"] = df["market_value_change"]
        #     df["Priority"] = df["priority_score"]
        #     df["Impact Allocation"] = df["impact_allocation"]
        #     df["Impact Risk"] = df["impact_risk"]

        #     display_cols = ["Symbol", "Action", "% Change", "€ Change", "Reason",
        #                     "Impact Allocation", "Impact Risk", "Priority"]
        #     df_display = df[display_cols]

        #     # --- Styling functions ---
        #     def color_pct(val):
        #         # gradient from light gray to green/red
        #         if val > 0:
        #             return "background-color: #4CAF50; color: white; font-weight: bold"  # strong green
        #         elif val < 0:
        #             return "background-color: #f44336; color: white; font-weight: bold"  # strong red
        #         else:
        #             return "background-color: #dcdcdc; color: #222; font-weight: bold"  # gray for Hold

        #     def color_priority(val):
        #         return "color: #222; font-weight: bold; background-color: #f0f0f0"

        #     styled = (
        #         df_display.style
        #         .format({
        #             "% Change": "{:.2f}%",
        #             "€ Change": "€{:,.0f}",
        #             "Impact Allocation": "{:.2%}",
        #             "Impact Risk": "{:.2%}",
        #             "Priority": "{:.2f}"
        #         })
        #         .map(color_pct, subset=["% Change"])
        #         # .map(color_priority, subset=["Priority"])
        #         .set_properties(**{"font-family": "Arial, sans-serif", "font-size": "12pt"})
        #     )

        #     df_display = df_display.sort_values(by="Priority", ascending=False)

        #     st.dataframe(styled, use_container_width=True)

        display_rebalance_table(rebalance)

    else:
        st.info("No rebalancing suggestions available. Portfolio may already match targets.")


# ---------- TECHNICAL ANALYSIS ----------
with tabs[3]:

    st.header("Technical Analysis & KPIs")

    # --- Number formatting ---
    def fmt_number(val, precision=0, percent=False):
        if val is None:
            return "N/A"
        try:
            if percent:
                return f"{val:.{precision}%}"
            elif precision > 0:
                return f"{val:,.{precision}f}"
            else:
                return f"{val:,}"
        except Exception:
            return "N/A"

    # --- Security label mapping ---
    all_syms = mw.get_all_symbols()
    sec_label_map = {format_sec_label(sym): sym for sym in sorted(all_syms)}

    # --- Security selection for charts (first section) ---
    chart_syms_label = st.multiselect(
        "Select securities to chart (portfolio + watchlist)",
        options=list(sec_label_map.keys()),
        key="chart_syms",
        default=[]
    )
    chart_syms = [sec_label_map[lbl] for lbl in chart_syms_label]

    # --- Load price series and determine max date range ---
    series_map = {}
    min_date, max_date = None, None
    for sym in chart_syms:
        dfp = db.get_price_series(sym)
        if dfp.empty:
            continue
        dfp['date'] = pd.to_datetime(dfp['date'])
        dfp = dfp.set_index('date').sort_index()
        for c in ['open','high','low','close','adj_close','volume']:
            if c in dfp.columns:
                dfp[c] = pd.to_numeric(dfp[c], errors='coerce')
        series_map[sym] = dfp
        if min_date is None or dfp.index.min() < min_date:
            min_date = dfp.index.min()
        if max_date is None or dfp.index.max() > max_date:
            max_date = dfp.index.max()

    # --- Filters expander ---
    with st.expander("Chart Filters", expanded=False):
        start = st.date_input("Start", value=min_date.date() if min_date else dt.date.today(), key="chart_start")
        end = st.date_input("End", value=max_date.date() if max_date else dt.date.today(), key="chart_end")
        sma_opts = st.multiselect("SMAs", [5,10,50,100,200], default=[50,200], key="sma_opts")
        ema_opts = st.multiselect("EMAs", [5,10,20,50,100,200], default=None, key="ema_opts")
        show_bb = st.checkbox("Bollinger Bands", value=True, key="bb_opt")

    if not series_map:
        st.info("No price data for selected symbols & date range.")
    else:
        # --- Price chart with overlays ---
        fig_price = go.Figure()
        for sym, dfp in series_map.items():
            lbl = format_sec_label(sym)
            # Slice by selected date range
            dfp_sel = dfp.loc[start:end]
            fig_price.add_trace(go.Scatter(x=dfp_sel.index, y=dfp_sel['close'], name=f"{lbl} Price", mode='lines'))

            # SMA/EMA overlays
            for w in sma_opts:
                s = mw.sma(dfp_sel['close'], w)
                fig_price.add_trace(go.Scatter(x=dfp_sel.index, y=s, name=f"{lbl} SMA{w}", mode='lines', line=dict(dash='dash')))
            for e in ema_opts:
                e_series = mw.ema(dfp_sel['close'], e)
                fig_price.add_trace(go.Scatter(x=dfp_sel.index, y=e_series, name=f"{lbl} EMA{e}", mode='lines', line=dict(dash='dot')))

            # Bollinger Bands
            if show_bb:
                m, up, low = mw.bollinger(dfp_sel['close'], 20, 2)
                fig_price.add_trace(go.Scatter(x=dfp_sel.index, y=up, fill=None, line=dict(dash='dash'), name=f"{lbl} BB Up"))
                fig_price.add_trace(go.Scatter(x=dfp_sel.index, y=low, fill='tonexty', line=dict(dash='dash'), name=f"{lbl} BB Low"))

            # Local min/max
            mins, maxs = mw.local_min_max(dfp_sel['close'], order=5)
            if len(mins):
                fig_price.add_trace(go.Scatter(x=dfp_sel.index[mins], y=dfp_sel['close'].iloc[mins],
                                               mode='markers', marker=dict(color='green', size=6), name=f"{lbl} Local Min"))
            if len(maxs):
                fig_price.add_trace(go.Scatter(x=dfp_sel.index[maxs], y=dfp_sel['close'].iloc[maxs],
                                               mode='markers', marker=dict(color='red', size=6), name=f"{lbl} Local Max"))

            # Compute SMA/EMA crossovers only if at least one SMA and one EMA are selected
            if sma_opts and ema_opts:
                for w in sma_opts:
                    s = mw.sma(dfp_sel['close'], w)
                    for e in ema_opts:
                        em = mw.ema(dfp_sel['close'], e)

                        # Find crossover points using your existing function
                        buy_idx, sell_idx = mw.find_crossovers(s, em)

                        # Plot buy signals (triangle-up)
                        if buy_idx:
                            fig_price.add_trace(go.Scatter(
                                x=dfp_sel.index[buy_idx],
                                y=dfp_sel['close'].iloc[buy_idx],
                                mode='markers',
                                marker=dict(symbol='triangle-up', color='green', size=8),
                                name=f"{lbl} SMA{w}↑EMA{e}"
                            ))

                        # Plot sell signals (triangle-down)
                        if sell_idx:
                            fig_price.add_trace(go.Scatter(
                                x=dfp_sel.index[sell_idx],
                                y=dfp_sel['close'].iloc[sell_idx],
                                mode='markers',
                                marker=dict(symbol='triangle-down', color='red', size=8),
                                name=f"{lbl} SMA{w}↓EMA{e}"
                            ))

        fig_price.update_layout(title="Price series with overlays", xaxis_title="Date", yaxis_title="Price",
                                height=420, legend=dict(orientation="v", y=1, x=1.02))
        st.plotly_chart(fig_price, use_container_width=True)

        # --- Grouped expanders for all securities ---
        with st.expander("Candlestick Charts", expanded=False):
            fig_candle = go.Figure()
            for sym, dfp in series_map.items():
                lbl = format_sec_label(sym)
                dfp_sel = dfp.loc[start:end]
                fig_candle.add_trace(go.Candlestick(x=dfp_sel.index,
                                                    open=dfp_sel['open'], high=dfp_sel['high'],
                                                    low=dfp_sel['low'], close=dfp_sel['close'],
                                                    name=lbl))
            st.plotly_chart(fig_candle, use_container_width=True)

        with st.expander("RSI Charts", expanded=False):
            fig_rsi = go.Figure()
            for sym, dfp in series_map.items():
                lbl = format_sec_label(sym)
                dfp_sel = dfp.loc[start:end]
                rsi_series = mw.rsi(dfp_sel['close']).dropna()
                fig_rsi.add_trace(go.Scatter(x=rsi_series.index, y=rsi_series, name=f"{lbl} RSI"))
                fig_rsi.add_hline(y=70, line_dash="dash", line_color="red")
                fig_rsi.add_hline(y=30, line_dash="dash", line_color="green")
                crosses_up = rsi_series[(rsi_series.shift(1) < 30) & (rsi_series >= 30)]
                crosses_down = rsi_series[(rsi_series.shift(1) > 70) & (rsi_series <= 70)]
                fig_rsi.add_trace(go.Scatter(x=crosses_up.index, y=crosses_up.values, mode='markers',
                                             marker=dict(color='green', size=8), name=f"{lbl} 30↑"))
                fig_rsi.add_trace(go.Scatter(x=crosses_down.index, y=crosses_down.values, mode='markers',
                                             marker=dict(color='red', size=8), name=f"{lbl} 70↓"))
            st.plotly_chart(fig_rsi, use_container_width=True)

        with st.expander("Return Distributions & Stats (Interactive)", expanded=False):
            for sym, dfp in series_map.items():
                lbl = format_sec_label(sym)
                dfp_sel = dfp.loc[start:end]

                # Daily returns
                returns = dfp_sel['close'].pct_change().dropna()
                mean_ret = returns.mean()
                std_ret = returns.std()
                skew_ret = skew(returns)
                kurt_ret = kurtosis(returns)

                # Display stats
                st.markdown(f"### {lbl} Daily Returns Stats")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Mean", f"{mean_ret:.4%}")
                col2.metric("Std Dev", f"{std_ret:.4%}")
                col3.metric("Skew", f"{skew_ret:.2f}")
                col4.metric("Kurtosis", f"{kurt_ret:.2f}")

                # Histogram with Plotly
                hist = go.Histogram(
                    x=returns,
                    nbinsx=50,
                    name="Returns",
                    marker_color='skyblue',
                    opacity=0.7,
                    histnorm='probability density'
                )

                # Normal distribution curve
                x = np.linspace(returns.min(), returns.max(), 200)
                y = norm.pdf(x, mean_ret, std_ret)
                normal_curve = go.Scatter(
                    x=x, y=y,
                    mode='lines',
                    line=dict(color='red', dash='dash'),
                    name='Normal Dist.'
                )

                # σ bands
                sigma_lines = []
                for k in [1, 2]:
                    for val in [mean_ret + k*std_ret, mean_ret - k*std_ret]:
                        sigma_lines.append(
                            go.Scatter(
                                x=[val, val],
                                y=[0, max(y)*1.1],
                                mode='lines',
                                line=dict(color='orange', dash='dot'),
                                name=f'±{k}σ'
                            )
                        )

                fig = go.Figure([hist, normal_curve] + sigma_lines)
                fig.update_layout(
                    title=f"Histogram of Daily Returns: {lbl}",
                    xaxis_title="Daily Return",
                    yaxis_title="Density",
                    template="plotly_white",
                    bargap=0.1,
                    showlegend=True
                )

                st.plotly_chart(fig, use_container_width=True)

        with st.expander("Volume Charts", expanded=False):
            fig_vol = go.Figure()
            for sym, dfp in series_map.items():
                lbl = format_sec_label(sym)
                dfp_sel = dfp.loc[start:end]
                fig_vol.add_trace(go.Bar(x=dfp_sel.index, y=dfp_sel['volume'].fillna(0).astype(float),
                                         name=lbl, text=dfp_sel['volume'], textposition='outside'))
            fig_vol.update_layout(height=350)
            st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("---")

    # --- KPIs and Comparisons (second section) ---
    st.subheader("📊 Technical KPIs & Financial Overview")

    if not chart_syms:
        st.info("Select at least one security above to view KPIs and financial data.")
    else:
        # --- Helper to format numbers ---
        def fmt_large(val):
            """Format big monetary values in millions or billions with EUR symbol."""
            if val is None or (isinstance(val, (int, float)) and np.isnan(val)):
                return "N/A"
            try:
                if abs(val) >= 1e9:
                    return f"€{val/1e9:,.2f} B"
                elif abs(val) >= 1e6:
                    return f"€{val/1e6:,.2f} M"
                else:
                    return f"€{val:,.0f}"
            except Exception:
                return "N/A"

        # Fetch data for all selected securities
        kpi_data = {sym: mw.get_security_basic(sym) for sym in chart_syms}

        # --- Single Security Detailed View ---
        if len(chart_syms) == 1:
            sym = chart_syms[0]
            data = kpi_data[sym]
            lbl = format_sec_label(sym)

            st.title(f"📈 Securities Overview — {lbl}")
            st.markdown(f"**{data.get('longName','')}**")

            dfp = series_map.get(sym)
            dfp_sel = dfp.loc[start:end] if dfp is not None else None

            # --- RISK & PERFORMANCE KPIs ---
            with st.expander("Risk & Performance KPIs", expanded=True):
                if dfp_sel is not None:
                    vol = mw.volatility(dfp_sel)
                    max_dd = mw.max_drawdown(dfp_sel['close'])
                    sharpe = mw.sharpe_ratio(dfp_sel)
                    sortino = mw.sortino_ratio(dfp_sel)
                    cagr = mw.cagr(dfp_sel)
                    calmar = mw.calmar_ratio(dfp_sel)
                    treynor = mw.treynor_ratio(dfp_sel)
                    info_ratio = mw.information_ratio(dfp_sel, benchmark_df) if 'benchmark_df' in locals() else np.nan

                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Volatility", f"{vol:.2%}")
                    col2.metric("Sharpe", f"{sharpe:.2f}")
                    col3.metric("Sortino", f"{sortino:.2f}")
                    col4.metric("Max Drawdown", f"{max_dd:.2%}")

                    col5, col6, col7, col8 = st.columns(4)
                    col5.metric("CAGR", f"{cagr:.2%}")
                    col6.metric("Calmar", f"{calmar:.2f}")
                    col7.metric("Treynor", f"{treynor:.2f}")
                    col8.metric("Info Ratio", f"{info_ratio:.2f}")

            # --- MARKET DATA ---
            with st.expander("Market Data", expanded=True):
                low, high, current = data.get("fiftyTwoWeekLow"), data.get("fiftyTwoWeekHigh"), data.get("regularMarketPrice")
                if all(v is not None for v in (low, high, current)):
                    cols = st.columns(3)
                    cols[0].metric("52W Low", fmt_large(low))
                    cols[1].metric("Price", fmt_large(current))
                    cols[2].metric("52W High", fmt_large(high))

            # --- VALUATION ---
            with st.expander("Valuation", expanded=False):
                cols = st.columns(3)
                cols[0].metric("Trailing PE", fmt_number(data.get('trailingPE'),2))
                cols[1].metric("Forward PE", fmt_number(data.get('forwardPE'),2))
                cols[2].metric("Enterprise Value", fmt_large(data.get('enterpriseValue')))

                cols = st.columns(2)
                cols[0].metric("Profit Margin", fmt_number(data.get('profitMargins'),2, percent=True))
                cols[1].metric("Operating Margin", fmt_number(data.get('operatingMargins'),2, percent=True))

            # --- REVENUE & PROFITS ---
            with st.expander("Revenue & Profits", expanded=False):
                st.metric("Total Revenue", fmt_large(data.get('totalRevenue')))
                cols = st.columns(2)
                cols[0].metric("Revenue per Share", fmt_number(data.get('revenuePerShare'),2))
                cols[1].metric("Gross Profits", fmt_large(data.get('grossProfits')))
                st.metric("EBITDA", fmt_large(data.get('ebitda')))

            # --- BALANCE SHEET ---
            with st.expander("Balance Sheet", expanded=False):
                cols = st.columns(4)
                cols[0].metric("Total Cash", fmt_large(data.get('totalCash')))
                cols[1].metric("Total Debt", fmt_large(data.get('totalDebt')))
                cols[2].metric("Current Ratio", fmt_number(data.get('currentRatio'),2))
                cols[3].metric("Book Value", fmt_number(data.get('bookValue'),2))

            # --- CASH FLOW ---
            with st.expander("Cash Flow", expanded=False):
                cols = st.columns(2)
                cols[0].metric("Operating Cash Flow", fmt_large(data.get('operatingCashflow')))
                cols[1].metric("Free Cash Flow", fmt_large(data.get('freeCashflow')))

            # --- SHARES ---
            with st.expander("Shares", expanded=False):
                cols = st.columns(2)
                cols[0].metric("Shares Outstanding", fmt_large(data.get('sharesOutstanding')))
                cols[1].metric("Market Cap", fmt_large(data.get('marketCap')))

        # --- MULTI-SECURITY COMPARISON VIEW ---
        else:
            st.markdown("### 📊 Multi-Security KPI Comparison")

            # Build a list of comparison dictionaries per security
            comp_rows = []
            for sym, data in kpi_data.items():
                dfp = series_map.get(sym)
                dfp_sel = dfp.loc[start:end] if dfp is not None else None

                if dfp_sel is not None:
                    vol = mw.volatility(dfp_sel)
                    max_dd = mw.max_drawdown(dfp_sel['close'])
                    sharpe = mw.sharpe_ratio(dfp_sel)
                    sortino = mw.sortino_ratio(dfp_sel)
                    cagr = mw.cagr(dfp_sel)
                    calmar = mw.calmar_ratio(dfp_sel)
                    treynor = mw.treynor_ratio(dfp_sel)
                    info_ratio = mw.information_ratio(dfp_sel, benchmark_df) if 'benchmark_df' in locals() else np.nan
                else:
                    vol = max_dd = sharpe = sortino = cagr = calmar = treynor = info_ratio = np.nan

                comp_rows.append({
                    "Symbol": sym,
                    # --- Risk & Performance ---
                    "Volatility": f"{vol:.2%}" if pd.notna(vol) else "N/A",
                    "Sharpe": f"{sharpe:.2f}" if pd.notna(sharpe) else "N/A",
                    "Sortino": f"{sortino:.2f}" if pd.notna(sortino) else "N/A",
                    "CAGR": f"{cagr:.2%}" if pd.notna(cagr) else "N/A",
                    "Calmar": f"{calmar:.2f}" if pd.notna(calmar) else "N/A",
                    "Treynor": f"{treynor:.2f}" if pd.notna(treynor) else "N/A",
                    "Info Ratio": f"{info_ratio:.2f}" if pd.notna(info_ratio) else "N/A",
                    "Max DD": f"{max_dd:.2%}" if pd.notna(max_dd) else "N/A",
                    # --- Market Data ---
                    "Price": fmt_large(data.get("regularMarketPrice")),
                    "52W Low": fmt_large(data.get("fiftyTwoWeekLow")),
                    "52W High": fmt_large(data.get("fiftyTwoWeekHigh")),
                    # --- Valuation ---
                    "Trailing PE": fmt_number(data.get("trailingPE"),2),
                    "Forward PE": fmt_number(data.get("forwardPE"),2),
                    "Enterprise Value": fmt_large(data.get("enterpriseValue")),
                    "Profit Margin": fmt_number(data.get("profitMargins"),2, percent=True),
                    "Operating Margin": fmt_number(data.get("operatingMargins"),2, percent=True),
                    # --- Revenue & Profits ---
                    "Total Revenue": fmt_large(data.get("totalRevenue")),
                    "Gross Profits": fmt_large(data.get("grossProfits")),
                    "EBITDA": fmt_large(data.get("ebitda")),
                    "Revenue/Share": fmt_number(data.get("revenuePerShare"),2),
                    # --- Balance Sheet ---
                    "Total Cash": fmt_large(data.get("totalCash")),
                    "Total Debt": fmt_large(data.get("totalDebt")),
                    "Current Ratio": fmt_number(data.get("currentRatio"),2),
                    "Book Value": fmt_number(data.get("bookValue"),2),
                    # --- Cash Flow ---
                    "Operating CF": fmt_large(data.get("operatingCashflow")),
                    "Free CF": fmt_large(data.get("freeCashflow")),
                    # --- Shares ---
                    "Shares Outstanding": fmt_large(data.get("sharesOutstanding")),
                    "Market Cap": fmt_large(data.get("marketCap")),
                })

            df_comp = pd.DataFrame(comp_rows).set_index("Symbol")

            # --- Show grouped expanders like single view ---
            with st.expander("Risk & Performance KPIs", expanded=True):
                cols = ["Volatility", "Sharpe", "Sortino", "CAGR", "Calmar", "Treynor", "Info Ratio", "Max DD"]
                st.dataframe(df_comp[cols], use_container_width=True)

            with st.expander("Market Data", expanded=True):
                cols = ["Price", "52W Low", "52W High"]
                st.dataframe(df_comp[cols], use_container_width=True)

            with st.expander("Valuation", expanded=False):
                cols = ["Trailing PE", "Forward PE", "Enterprise Value", "Profit Margin", "Operating Margin"]
                st.dataframe(df_comp[cols], use_container_width=True)

            with st.expander("Revenue & Profits", expanded=False):
                cols = ["Total Revenue", "Gross Profits", "EBITDA", "Revenue/Share"]
                st.dataframe(df_comp[cols], use_container_width=True)

            with st.expander("Balance Sheet", expanded=False):
                cols = ["Total Cash", "Total Debt", "Current Ratio", "Book Value"]
                st.dataframe(df_comp[cols], use_container_width=True)

            with st.expander("Cash Flow", expanded=False):
                cols = ["Operating CF", "Free CF"]
                st.dataframe(df_comp[cols], use_container_width=True)

            with st.expander("Shares", expanded=False):
                cols = ["Shares Outstanding", "Market Cap"]
                st.dataframe(df_comp[cols], use_container_width=True)



    # # --- KPIs for selected security (second section) ---
    # selected_label = st.selectbox(
    #     "Select security for KPI table (leave blank for none)",
    #     options=[""] + list(sec_label_map.keys()),
    #     key="kpi_select_sym"
    # )

    # if selected_label:
    #     kpi_sym = sec_label_map[selected_label]
    #     data = mw.get_security_basic(kpi_sym)

    #     st.title(f"📈 Securities Overview — {kpi_sym}")
    #     st.markdown(f"**{data.get('longName','')}**")

    #     # --- Risk & Performance KPIs ---
    #     with st.expander("Risk & Performance KPIs", expanded=True):
    #         for sym, dfp in series_map.items():
    #             lbl = format_sec_label(sym)
    #             dfp_sel = dfp.loc[start:end]

    #             vol = mw.volatility(dfp_sel)
    #             max_dd = mw.max_drawdown(dfp_sel['close'])
    #             sharpe = mw.sharpe_ratio(dfp_sel)
    #             sortino = mw.sortino_ratio(dfp_sel)
    #             cagr = mw.cagr(dfp_sel)
    #             calmar = mw.calmar_ratio(dfp_sel)
    #             treynor = mw.treynor_ratio(dfp_sel)

    #             if 'benchmark_df' in locals() and benchmark_df is not None:
    #                 # For Info Ratio, pass a benchmark df (e.g. market index)
    #                 info_ratio = mw.information_ratio(dfp_sel, benchmark_df)
    #             else:
    #                 info_ratio = np.nan
                
    #             st.markdown(f"### {lbl}")

    #             col1, col2, col3, col4 = st.columns(4)
    #             col1.metric("Volatility", f"{vol:.2%}", "Lower is safer")
    #             col2.metric("Sharpe Ratio", f"{sharpe:.2f}", ">1 good, >2 very good")
    #             col3.metric("Sortino Ratio", f"{sortino:.2f}", ">1 good, >2 very good")
    #             col4.metric("Max Drawdown", f"{max_dd:.2%}", "Lower is better")

    #             col5, col6, col7, col8 = st.columns(4)
    #             col5.metric("CAGR", f"{cagr:.2%}", "Positive = growth")
    #             col6.metric("Calmar Ratio", f"{calmar:.2f}", ">0.5 decent, >1 strong")
    #             col7.metric("Treynor Ratio", f"{treynor:.2f}", "Higher is better")
    #             col8.metric("Info Ratio", f"{info_ratio:.2f}", ">0.5 good, >1 very good")

    #     # MARKET DATA
    #     with st.expander("Market Data", expanded=True):
    #         low = data.get("fiftyTwoWeekLow")
    #         high = data.get("fiftyTwoWeekHigh")
    #         current = data.get("regularMarketPrice")
    #         if all(v is not None for v in (low, high, current)):
    #             cols = st.columns(3)
    #             cols[0].metric("52W Low", f"€{fmt_number(low,2)}")
    #             cols[1].metric("Price", f"€{fmt_number(current,2)}")
    #             cols[2].metric("52W High", f"€{fmt_number(high,2)}")

    #     # --- DIVIDENDS ---
    #     with st.expander("Dividends", expanded=False):
    #         cols = st.columns(2)
    #         cols[0].metric("Dividend Rate", fmt_number(data.get('dividendRate'),2))
    #         cols[1].metric("Dividend Yield", fmt_number(data.get('dividendYield'),2, percent=True))

    #     # --- VALUATION ---
    #     with st.expander("Valuation", expanded=False):
    #         cols = st.columns(3)
    #         cols[0].metric("Trailing PE", fmt_number(data.get('trailingPE'),2))
    #         cols[1].metric("Forward PE", fmt_number(data.get('forwardPE'),2))
    #         cols[2].metric("Enterprise Value", fmt_number(data.get('enterpriseValue')))

    #         cols = st.columns(2)
    #         cols[0].metric("Profit Margin", fmt_number(data.get('profitMargins'),2, percent=True))
    #         cols[1].metric("Operating Margin", fmt_number(data.get('operatingMargins'),2, percent=True))

    #     # --- REVENUE & PROFITS ---
    #     with st.expander("Revenue & Profits", expanded=False):
    #         st.metric("Total Revenue", fmt_number(data.get('totalRevenue')))
    #         cols = st.columns(2)
    #         cols[0].metric("Revenue per Share", fmt_number(data.get('revenuePerShare'),2))
    #         cols[1].metric("Gross Profits", fmt_number(data.get('grossProfits')))
    #         st.metric("EBITDA", fmt_number(data.get('ebitda')))

    #     # --- BALANCE SHEET ---
    #     with st.expander("Balance Sheet", expanded=False):
    #         cols = st.columns(4)
    #         cols[0].metric("Total Cash", fmt_number(data.get('totalCash')))
    #         cols[1].metric("Total Debt", fmt_number(data.get('totalDebt')))
    #         cols[2].metric("Current Ratio", fmt_number(data.get('currentRatio'),2))
    #         cols[3].metric("Book Value", fmt_number(data.get('bookValue'),2))

    #     # --- CASH FLOW ---
    #     with st.expander("Cash Flow", expanded=False):
    #         cols = st.columns(2)
    #         cols[0].metric("Operating Cash Flow", fmt_number(data.get('operatingCashflow')))
    #         cols[1].metric("Free Cash Flow", fmt_number(data.get('freeCashflow')))

    #     # --- SHARES ---
    #     with st.expander("Shares", expanded=False):
    #         cols = st.columns(2)
    #         cols[0].metric("Shares Outstanding", fmt_number(data.get('sharesOutstanding')))
    #         cols[1].metric("Market Cap", fmt_number(data.get('marketCap')))

# ---------- NEWS & SENTIMENT ----------
with tabs[4]:
    st.info("Coming soon...")
#     st.header("News & Sentiment")
#     st.info("Portfolio-level sentiment overview and per-symbol deep dive.")
#     p_df = pd.read_sql_query("SELECT id,name FROM portfolios ORDER BY name", conn)
#     all_names = p_df['name'].tolist() if not p_df.empty else []
#     sel_ports = st.multiselect("Select portfolios (default=All)", options=all_names, default=all_names, key="news_pf_sel")
#     port_ids = [int(p_df[p_df['name']==n]['id'].iloc[0]) for n in sel_ports] if sel_ports else None
#     syms = set()
#     try:
#         for r in conn.execute("SELECT symbol FROM securities WHERE watchlist=1"):
#             syms.add(r[0])
#     except Exception: pass
#     try:
#         if port_ids:
#             for pid in port_ids:
#                 for r in conn.execute("SELECT DISTINCT symbol FROM transactions WHERE portfolio_id=?", (pid,)):
#                     syms.add(r[0])
#         else:
#             for r in conn.execute("SELECT DISTINCT symbol FROM transactions"):
#                 syms.add(r[0])
#     except Exception: pass
#     syms = sorted([s for s in syms if s])
#     if not syms:
#         st.info("No symbols in selection")
#     else:
#         rows=[]
#         for s in syms:
#             q = conn.execute("SELECT sentiment_label,sentiment_score FROM news WHERE symbol=?", (s,))
#             items = q.fetchall()
#             total = len(items)
#             pos = sum(1 for it in items if it['sentiment_label']=='positive')
#             neu = sum(1 for it in items if it['sentiment_label']=='neutral')
#             neg = sum(1 for it in items if it['sentiment_label']=='negative')
#             avg = None
#             scores = [float(it['sentiment_score']) for it in items if it['sentiment_score'] is not None]
#             if scores:
#                 avg = sum(scores)/len(scores)
#             label = 'neutral'
#             if avg is not None:
#                 label = 'positive' if avg>=0.05 else ('negative' if avg<=-0.05 else 'neutral')
#             rows.append({'symbol': s, 'total': total, 'positive': pos, 'neutral': neu, 'negative': neg, 'avg': avg, 'label': label})
#         df_sent = pd.DataFrame(rows).sort_values('total', ascending=False)
#         st.dataframe(df_sent[['symbol','total','positive','neutral','negative','avg','label']], use_container_width=True)
#         sel_sym = st.selectbox("Select symbol for articles", options=[""] + df_sent['symbol'].tolist(), key="news_sel_sym")
#         if sel_sym:
#             news_df = pd.read_sql_query("SELECT id,title,publisher,link,published_at,sentiment_score,sentiment_label,article_text FROM news WHERE symbol=? ORDER BY published_at DESC LIMIT ?", conn, params=(sel_sym, int(CFG.get('news_max_items',50))))
#             if news_df.empty:
#                 st.info("No news for this symbol")
#             else:
#                 for idx, row in news_df.iterrows():
#                     label = row['sentiment_label'] or 'N/A'; score = row['sentiment_score']; title = row['title'] or "(no title)"
#                     emoji = "👍" if label=='positive' else ("😐" if label=='neutral' else "👎")
#                     with st.expander(f"{emoji} {title} — {label} ({score})", expanded=False):
#                         st.markdown(f"**Publisher:** {row['publisher']}  \n**Date:** {row['published_at']}")
#                         if row['link']:
#                             st.markdown(f"[Open original]({row['link']})")
#                         if row['article_text']:
#                             st.text_area("Article text", value=row['article_text'], height=260, key=f"art_{sel_sym}_{idx}")
#                         else:
#                             st.write("No extracted article text available.")

# ---------- TRANSACTIONS ----------
with tabs[5]:
    st.header("Transactions & Portfolios")

    df_tx_all = db.list_transactions_detailed()

    if df_tx_all.empty:
        st.info("No transactions recorded")
    else:
        # Security + portfolio metadata
        sec_df_all = db.list_securities()
        p_df = db.list_portfolios()
        port_options = p_df['name'].tolist() if not p_df.empty else []

        # Build label mappings
        symbol_to_label = {r['symbol']: format_sec_label(r['symbol']) for _, r in sec_df_all.iterrows()}
        label_to_symbol = {v: k for k, v in symbol_to_label.items()}

        # --- Filters ---
        with st.expander("Filters", expanded=False):
            sel_ports = st.multiselect("Select portfolios", options=port_options, default=[], key="tx_port_sel")
            port_ids = p_df[p_df['name'].isin(sel_ports)]['id'].tolist() if sel_ports else None

            available_syms = sorted(df_tx_all['symbol'].dropna().unique().tolist())
            sel_syms = st.multiselect(
                "Select securities",
                options=[symbol_to_label[s] for s in available_syms],
                default=[],
                key="tx_sec_sel"
            )
            sel_syms = [label_to_symbol[lbl] for lbl in sel_syms] if sel_syms else None

            all_types = df_tx_all['tx_type'].dropna().unique().tolist()
            sel_types = st.multiselect("Select transaction types", options=all_types, default=[], key="tx_type_sel")

        df_tx = df_tx_all.copy()
        if port_ids:
            df_tx = df_tx[df_tx['portfolio_id'].isin(port_ids)]
        if sel_syms:
            df_tx = df_tx[df_tx['symbol'].isin(sel_syms)]
        if sel_types:
            df_tx = df_tx[df_tx['tx_type'].isin(sel_types)]

        # Ensure datetime
        df_tx['tx_date'] = pd.to_datetime(df_tx['tx_date'], errors='coerce')
        df_tx['security_label'] = df_tx['symbol'].apply(lambda s: symbol_to_label.get(s, s))
        df_tx['total_cost'] = df_tx['quantity'] * df_tx['price'] + df_tx['tx_cost']

        num_buys = len(df_tx[df_tx['tx_type'] == 'buy'])
        num_sells = len(df_tx[df_tx['tx_type'] == 'sell'])

        st.subheader("Summary")
        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        c1.metric("Transactions", len(df_tx))
        c2.metric("Total Quantity", f"{df_tx['quantity'].sum():,.4f}")
        c3.metric("Total Buys (€)", f"€{df_tx[df_tx['tx_type'] == 'buy']['total_cost'].sum():,.2f}")
        c4.metric("Number of Buys", num_buys)
        c5.metric("Total Sales (€)", f"€{df_tx[df_tx['tx_type'] == 'sell']['total_cost'].sum():,.2f}")
        c6.metric("Number of Sells", num_sells)
        c7.metric("Total Fees (€)", f"€{df_tx['tx_cost'].sum():,.2f}")


        with st.expander("Transaction Analysis & Trends", expanded=False):
            st.subheader("Transaction Trends & Fee Analysis")

            if not df_tx.empty:
                df_tx['tx_date'] = pd.to_datetime(df_tx['tx_date'], errors='coerce')

                # --- Average Fee per Unit (scatter/bar for sparse data) ---
                df_tx['fee_per_unit'] = df_tx['tx_cost'] / df_tx['quantity'].replace(0, np.nan)
                fee_df = df_tx.groupby(pd.Grouper(key='tx_date', freq='M'))['fee_per_unit'].mean().reset_index()
                fig_fee_avg = px.bar(
                    fee_df, x='tx_date', y='fee_per_unit',
                    title="Average Fees per Unit Over Time",
                    labels={'tx_date': 'Month', 'fee_per_unit': 'Avg Fee per Unit (€)'},
                    text=fee_df['fee_per_unit'].apply(lambda x: f"€{x:.2f}" if pd.notna(x) else "")
                )
                fig_fee_avg.update_traces(marker_color='orange', textposition='outside')
                
                # --- Cumulative Fee per Unit over time (area chart with trendline) ---
                df_tx_sorted = df_tx.sort_values('tx_date')
                df_tx_sorted['cum_quantity'] = df_tx_sorted['quantity'].cumsum()
                df_tx_sorted['cum_fees'] = df_tx_sorted['tx_cost'].cumsum()
                df_tx_sorted['fee_per_unit_cum'] = df_tx_sorted['cum_fees'] / df_tx_sorted['cum_quantity'].replace(0, np.nan)

                fig_fee_cum = px.area(
                    df_tx_sorted, x='tx_date', y='fee_per_unit_cum',
                    title="Cumulative Fee per Unit Over Time",
                    labels={'tx_date': 'Date', 'fee_per_unit_cum': 'Cumulative Fee per Unit (€)'}
                )
                # Add simple trend indicator (linear fit)
                trend = np.polyfit(df_tx_sorted.index, df_tx_sorted['fee_per_unit_cum'].fillna(0), 1)
                fig_fee_cum.add_trace(go.Scatter(
                    x=df_tx_sorted['tx_date'], 
                    y=trend[0] * df_tx_sorted.index + trend[1],
                    mode='lines',
                    name='Trend',
                    line=dict(color='red', dash='dash')
                ))

                # --- Monthly Trading Volume by Type (bar chart with labels) ---
                volume_df = df_tx.groupby([pd.Grouper(key='tx_date', freq='M'), 'tx_type'])['quantity'].sum().reset_index()
                fig_volume = px.bar(
                    volume_df,
                    x='tx_date', y='quantity', color='tx_type',
                    title="Monthly Trading Volume by Type",
                    labels={'tx_date': 'Month', 'quantity': 'Quantity', 'tx_type': 'Type'},
                    text='quantity',
                    barmode='group'
                )
                fig_volume.update_traces(texttemplate='%{text:.2f}', textposition='outside')

                # --- Cumulative Buys/Sells (stacked area) ---
                df_tx_sorted['cumulative_buys'] = df_tx_sorted[df_tx_sorted['tx_type']=='buy']['quantity'].cumsum()
                df_tx_sorted['cumulative_sells'] = df_tx_sorted[df_tx_sorted['tx_type']=='sell']['quantity'].cumsum()
                df_tx_sorted['cumulative_buys'].fillna(method='ffill', inplace=True)
                df_tx_sorted['cumulative_sells'].fillna(method='ffill', inplace=True)

                cum_df = df_tx_sorted[['tx_date', 'cumulative_buys', 'cumulative_sells']].drop_duplicates()
                fig_cum_area = px.area(
                    cum_df,
                    x='tx_date',
                    y=['cumulative_buys', 'cumulative_sells'],
                    labels={'tx_date': 'Date', 'value': 'Cumulative Quantity', 'variable': 'Transaction Type'},
                    title="Cumulative Buy/Sell Quantities (Area Chart)"
                )

                # --- Display charts ---
                st.plotly_chart(fig_fee_avg, use_container_width=True)
                st.plotly_chart(fig_fee_cum, use_container_width=True)
                st.plotly_chart(fig_volume, use_container_width=True)
                st.plotly_chart(fig_cum_area, use_container_width=True)

            else:
                st.info("No transactions to analyze.")



        # Scrollable table inside an expander
        with st.expander("Transactions Overview", expanded=False):
            st.markdown(
                """
                <style>
                .scroll-table {
                    max-height: 400px;
                    overflow-y: auto;
                    padding-right: 10px;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            st.markdown('<div class="scroll-table">', unsafe_allow_html=True)

            display_cols = ['tx_date', 'security_label', 'portfolio', 'tx_type', 'quantity', 'price', 'tx_cost', 'total_cost']
            df_display = df_tx[display_cols + ['id']].sort_values('tx_date', ascending=False).reset_index(drop=True)

            # Column headers
            h1, h2, h3, h4, h5, h6, h7, h8, h9, h10 = st.columns([1.2, 2.5, 2, 1.2, 1.2, 1.5, 1.5, 1.5, 1, 1])
            h1.markdown("**Date**"); h2.markdown("**Security**"); h3.markdown("**Portfolio**"); h4.markdown("**Type**")
            h5.markdown("**Quantity**"); h6.markdown("**Price (€)**"); h7.markdown("**Fees (€)**"); h8.markdown("**Total (€)**")
            h9.markdown("**Edit**"); h10.markdown("**Delete**")

            for _, row in df_display.iterrows():
                st.divider()

                c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 = st.columns([1.2, 2.5, 2, 1.2, 1.2, 1.5, 1.5, 1.5, 1, 1])
                c1.write(row['tx_date'].strftime("%Y-%m-%d") if pd.notna(row['tx_date']) else "")
                c2.write(row['security_label'])
                c3.write(row['portfolio'])
                tx_type = str(row['tx_type']).lower()
                if tx_type == "buy":
                    display_type = "⬆️ <span style='color:green'>BUY</span>"
                elif tx_type == "sell":
                    display_type = "⬇️ <span style='color:red'>SELL</span>"
                else:
                    display_type = tx_type.upper()

                # Use markdown to render colored arrows
                c4.markdown(display_type, unsafe_allow_html=True)
                c5.write(f"{row['quantity']:.2f}")
                c6.write(f"€{row['price']:.2f}")
                c7.write(f"€{row['tx_cost']:.2f}")
                c8.write(f"€{row['total_cost']:.2f}")

                tx_id = row.get('id')
                edit_btn = c9.button("✏️", key=f"edit_{tx_id}") if tx_id is not None else None
                del_btn = c10.button("🗑️", key=f"del_{tx_id}") if tx_id is not None else None

                # Delete transaction immediately (with spinner & safe rerun)
                if del_btn and tx_id is not None:
                    with st.spinner("Deleting transaction…"):
                        mw.remove_transaction(int(tx_id))
                    st.success(f"Transaction {tx_id} deleted.")
                    st.rerun()

                # Edit inline
                if edit_btn:
                    st.session_state['edit_tx_id'] = int(row['id'])  

            st.markdown('</div>', unsafe_allow_html=True)

            # Edit form
            if 'edit_tx_id' in st.session_state:
                tx_id = st.session_state['edit_tx_id']
                row = df_tx[df_tx['id'] == tx_id].iloc[0]
                st.markdown(f"### Edit Transaction #{tx_id}")

                with st.form(f"edit_form_{tx_id}"):
                    sec_labels_all = ["-- create new security --"] + list(symbol_to_label.values())
                    curr_label = symbol_to_label.get(row['symbol'], row['security_label'])
                    sec_choice = st.selectbox(
                        "Security",
                        options=sec_labels_all,
                        index=sec_labels_all.index(curr_label) if curr_label in sec_labels_all else 0
                    )

                    if sec_choice == "-- create new security --":
                        new_sym = st.text_input("New symbol")
                        new_name = st.text_input("New name")
                        new_isin = st.text_input("New ISIN")
                    else:
                        sym = label_to_symbol[sec_choice]
                        meta = sec_df_all[sec_df_all['symbol'] == sym].iloc[0].to_dict()
                        new_sym = sym
                        new_name = st.text_input("Name", value=meta.get('name', ''))
                        new_isin = st.text_input("ISIN", value=meta.get('isin', ''))

                    port_choice = st.selectbox(
                        "Portfolio",
                        options=port_options,
                        index=port_options.index(row['portfolio']) if row['portfolio'] in port_options else 0
                    )
                    tx_date_val = st.date_input("Date", value=row['tx_date'].date() if pd.notna(row['tx_date']) else dt.date.today())
                    tx_type_val = st.selectbox("Type", options=["buy", "sell"], index=0 if row['tx_type'] == "buy" else 1)
                    qty_val = st.number_input("Quantity", value=float(row['quantity']), format="%.5f")
                    price_val = st.number_input("Price", value=float(row['price']), format="%.3f")
                    fees_val = st.number_input("Fees", value=float(row['tx_cost']), format="%.2f")

                    submitted = st.form_submit_button("Save changes")
                    if submitted:
                        port_id = int(p_df[p_df['name'] == port_choice]['id'].iloc[0])
                        sec_id = mw.add_new_security(new_sym, name=new_name, isin=new_isin) if sec_choice == "-- create new security --" else int(sec_df_all[sec_df_all['symbol'] == new_sym]['id'].iloc[0])

                        mw.edit_transaction(
                            tx_id=tx_id,
                            tx_date=tx_date_val.isoformat(),
                            tx_type=tx_type_val,
                            quantity=qty_val,
                            price=price_val,
                            fees=fees_val,
                            security_id=sec_id,
                            security_name=new_name,
                            security_isin=new_isin
                        )
                        st.success("Transaction updated.")
                        del st.session_state['edit_tx_id']
                        st.rerun()

    st.divider()




    # --- Add Transaction ---
    st.subheader("One-time Transaction")


    with st.expander("Add transaction", expanded=False):
        # Load existing securities for autocomplete
        all_syms = mw.get_all_symbols()
        label_map = {format_sec_label(sym): sym for sym in all_syms}
        labels = ["--- create new security ---"] + list(label_map.keys())

        selected_label = st.selectbox("Select or create security", options=labels, index=0, key="tx_select_security")

        if selected_label == "--- create new security ---":
            tx_symbol_val = ""
            tx_name_val = ""
            tx_isin_val = ""
        else:
            tx_symbol_val = label_map[selected_label]
            # Lookup directly in DB/middleware to get ISIN and Name
            row = db.get_security_by_symbol(tx_symbol_val)  # implement in your db
            if row:
                tx_isin_val = row.get("isin", "")
                tx_name_val = row.get("longName", "") or row.get("shortName", "")
                tx_name_val = f"{tx_name_val} ({tx_symbol_val})"
            else:
                tx_isin_val = ""
                tx_name_val = f"({tx_symbol_val})"


        with st.form("tx_one_form"):
            tx_symbol = st.text_input("Yahoo symbol", value=tx_symbol_val)
            # tx_name_input = st.text_input("Name", value=tx_name_val)
            tx_isin = st.text_input("ISIN", value=tx_isin_val)

            # Portfolio selection
            p_df = mw.list_portfolios()
            port_options = ["-- create new --"] + (p_df['name'].tolist() if not p_df.empty else [])
            tx_port_choice = st.selectbox("Portfolio", options=port_options, key="tx_one_port")
            is_new_port = (tx_port_choice == "-- create new --")
            tx_new_port = st.text_input("New portfolio name (if creating)", disabled=not is_new_port)

            tx_date = st.date_input("Date", value=dt.date.today(), key="tx_one_date")
            tx_type = st.selectbox("Type", ["buy", "sell"], key="tx_one_type")
            tx_qty = st.number_input("Quantity (shares)", min_value=0.0, value=1.0, step=1.0, format="%.5f")
            tx_amount = st.number_input("Price per share (leave 0 to use latest price)", min_value=0.0, value=0.0, format="%.3f")
            tx_fees = st.number_input("Fees", min_value=0.0, value=0.0)

            tx_submit = st.form_submit_button("Add transaction")

            if tx_submit:
                if not tx_symbol:
                    st.error("Symbol required")
                    st.stop()

                # Handle portfolio creation
                if is_new_port:
                    if not tx_new_port:
                        st.error("New portfolio name required")
                        st.stop()
                    portfolio_id = mw.create_portfolio(tx_new_port)
                else:
                    matching = p_df[p_df['name'] == tx_port_choice]
                    if matching.empty:
                        st.error(f"Portfolio '{tx_port_choice}' not found")
                        st.stop()
                    portfolio_id = int(matching['id'].iloc[0])

                # Handle new security creation
                if selected_label == "--- create new security ---":
                    mw.add_security(tx_symbol, tx_name_input, tx_isin)  # implement in mw/db
                else:
                    # ensure security exists in DB
                    sec_row = db.get_security_by_symbol(tx_symbol)
                    if not sec_row:
                        mw.add_security(tx_symbol, tx_name_input, tx_isin)

                # Determine price
                price_to_use = tx_amount
                if price_to_use == 0.0:
                    price_to_use = db.get_latest_price(tx_symbol)
                    if price_to_use is None:
                        st.error(f"No price available for {tx_symbol}")
                        st.stop()

                # Add transaction
                mw.add_transaction(
                    portfolio_id=portfolio_id,
                    symbol=tx_symbol,
                    tx_date=tx_date.isoformat(),
                    tx_type=tx_type,
                    quantity=tx_qty,
                    price=price_to_use,
                    fees=tx_fees
                )

                st.success("Transaction added")
                st.rerun()

    st.divider()


    # --- Manage portfolios ---   
    st.subheader("Manage Portfolios")

    with st.expander("Portfolio Management", expanded=False):

        col1, col2, col3 = st.columns(3)

        with col1:
            new_name = st.text_input("New Portfolio Name")
            if st.button("Create Portfolio"):
                if new_name:
                    mw.create_portfolio(new_name)
                    st.success(f"Portfolio '{new_name}' created.")
                    st.rerun()

        with col2:
            old_name = st.selectbox("Select portfolio to edit", options=p_df['name'].tolist(), key="sel_port_edit")#st.selectbox("Portfolio to Rename", [p["name"] for p in p_df])
            new_name = st.text_input("Rename To")
            if st.button("Rename Portfolio"):
                if new_name:
                    mw.rename_portfolio(old_name, new_name)
                    st.success(f"Portfolio '{old_name}' renamed to '{new_name}'.")
                    st.rerun()

        with col3:
            to_delete = st.selectbox("Select portfolio to delete", options=p_df['name'].tolist(), key="sel_port_delete")#st.selectbox("Portfolio to Delete", [p["name"] for p in p_df])
            target_name = st.selectbox("Select portfolio to re-assign securities", options=p_df['name'].tolist(), key="sel_port_reassign")#st.selectbox("Portfolio to Delete", [p["name"] for p in p_df])
            if st.button("Delete Portfolio"):
                mw.delete_and_reassign_portfolio(to_delete, target_name)
                st.warning(f"Portfolio '{to_delete}' deleted.")
                st.rerun()       
   

# ---------- WATCHLIST ----------
with tabs[6]:
    st.header("Watchlist")

    watch_df = mw.get_watchlist()

    if watch_df.empty:
        st.info("No watchlist items")
    else:

        with st.expander("Watchlist Filters", expanded=False):
            # --- Filters ---
            st.subheader("Filters")

            country_filter = st.multiselect(
                "Country",
                options=sorted(watch_df["country"].fillna("Unknown").unique()),
                key="watchlist_country_filter"
            )
            sector_filter = st.multiselect(
                "Sector",
                options=sorted(watch_df["sector"].fillna("Unknown").unique()),
                key="watchlist_sector_filter"
            )
            industry_filter = st.multiselect(
                "Industry",
                options=sorted(watch_df["industry"].fillna("Unknown").unique()),
                key="watchlist_industry_filter"
            )
            type_filter = st.multiselect(
                "Security Type",
                options=sorted(watch_df["security_type"].fillna("Unknown").unique()),
                key="watchlist_type_filter"
            )

            # Optional beta filter
            use_beta = st.checkbox("Enable Beta filter", value=False, key="watchlist_use_beta")
            beta_threshold = st.number_input(
                "Max Beta",
                value=1.0,
                step=0.1,
                format="%.2f",
                key="watchlist_beta_threshold"
            )


            # Apply filters (keep incomplete rows visible with N/A)
            df_filtered = watch_df.copy()

            if country_filter:
                df_filtered = df_filtered[df_filtered["country"].fillna("Unknown").isin(country_filter)]
            if sector_filter:
                df_filtered = df_filtered[df_filtered["sector"].fillna("Unknown").isin(sector_filter)]
            if industry_filter:
                df_filtered = df_filtered[df_filtered["industry"].fillna("Unknown").isin(industry_filter)]
            if type_filter:
                df_filtered = df_filtered[df_filtered["security_type"].fillna("Unknown").isin(type_filter)]
            if use_beta and "beta" in df_filtered.columns:
                df_filtered = df_filtered[df_filtered["beta"].fillna(9999) <= beta_threshold]

        with st.expander("Watchlist", expanded=True):
            # --- Display filtered table (always show incomplete rows with N/A) ---

            display_kpi_table(df_filtered)

            # --- Delete dropdown ---
            del_symbol = st.selectbox(
                "Select symbol to delete from watchlist",
                options=[
                    f"{row['security_name']} - {row['symbol']}"
                    for _, row in watch_df.iterrows()
                ],
                key="watch_del_symbol"
            )

            if st.button("Delete from watchlist", key="watch_del_btn"):
                if del_symbol:
                    # Extract symbol part (after last " - ")
                    symbol_to_delete = del_symbol.split(" - ")[-1]
                    mw.delete_security_from_watchlist(symbol_to_delete)
                    st.success(f"{del_symbol} deleted from watchlist")
                    st.rerun()

    with st.expander("Adding new Watchlist items", expanded=True):
        st.info("Add by Yahoo ticker only. Metadata auto-fetched by the fetcher.")

        st.subheader("Add to watchlist")
        with st.form("add_watch"):
            w_symbol = st.text_input("Yahoo symbol", key="watch_add_sym")
            w_submit = st.form_submit_button("Add")
            if w_submit:
                if not w_symbol:
                    st.error("Symbol required")
                else:
                    mw.add_security (w_symbol)
                    st.success("Added to watchlist (metadata will be fetched by data_fetcher)")
                    st.rerun()


# ---------- ALERTS MANAGER ----------
with tabs[7]:
    st.header("Alerts Manager")

    # --- Fetch alerts ---
    alerts = mw.get_alerts()
    if alerts.empty:
        st.info("No alerts configured yet.")
    else:
        # --- Filter expander ---
        with st.expander("Filters", expanded=False):
            # Security filter
            sec_labels = [format_sec_label(row['symbol']) for _, row in alerts.iterrows()]
            sel_secs = st.multiselect("Select securities", options=sorted(sec_labels), default=[])
            sel_sec_ids = [row['security_id'] for i, row in alerts.iterrows() if format_sec_label(row['symbol']) in sel_secs] if sel_secs else None

            # Alert type filter
            alert_types = sorted(alerts['alert_type'].dropna().unique().tolist())
            sel_types = st.multiselect("Alert types", options=alert_types, default=[])

            # Active / inactive filter
            sel_active = st.multiselect("Status", options=["Active","Inactive"], default=[])

        # --- Apply filters ---
        df_filtered = alerts.copy()
        if sel_sec_ids:
            df_filtered = df_filtered[df_filtered['security_id'].isin(sel_sec_ids)]
        if sel_types:
            df_filtered = df_filtered[df_filtered['alert_type'].isin(sel_types)]
        if sel_active:
            active_map = {"Active": True, "Inactive": False}
            selected_bool = [active_map[s] for s in sel_active]
            df_filtered = df_filtered[df_filtered['active'].isin(selected_bool)]


        # --- Description at bottom in collapsible info box ---
        with st.expander("ℹ️ Alert Type Descriptions", expanded=False):
            st.markdown("""
            **price** — when current price crosses threshold. Absolute or percentage.  
            **mos** — triggers when margin-of-safety (DCF) >= threshold fraction.  
            **rsi** — triggers when RSI crosses underbought/overbought thresholds.  
            **ma_crossover** — short/long moving average cross (golden/death).  
            **52w** — 52-week high/low.  
            **volume_spike** — today's volume >= multiplier * avg(volume).  
            **dividend/earnings** — when such event occurred in last N days.
            """)


        st.subheader(f"Showing {len(df_filtered)} Alerts")

        # --- Alerts List in expander ---
        # with st.expander("Alerts List", expanded=True):
        #     for idx, row in df_filtered.iterrows():
        #         if idx > 0:  # skip divider for first alert
        #             st.divider()
                
        #         sec_label = format_sec_label(row['symbol'])
        #         st.markdown(f"### {sec_label}")  # Full-width title

        #         # Fields below title
        #         cols = st.columns([1,1,1,1,1,1,1])
        #         cols[0].markdown(f"**Type:** {row['alert_type']}")
        #         cols[1].markdown(f"**Active:** {'✅' if row['active'] else '❌'}")
        #         cols[2].markdown(f"**Notify:** {row['notify_mode']}")
        #         cols[3].markdown(f"**Cooldown:** {row['cooldown_seconds']}s")
        #         cols[4].markdown(f"**Last triggered:** {row['last_triggered'] or '—'}")
        #         cols[5].markdown(f"**Note:** {row.get('note','—')}")

        #         # Display thresholds / parameters
        #         params = json.loads(row["params"]) if isinstance(row["params"], str) else row["params"] or {}
        #         if params:
        #             param_str = ", ".join(f"{k}={v}" for k, v in params.items())
        #             st.markdown(f"**Thresholds / Parameters:** {param_str}")

        #         # Action buttons with unique keys
        #         bcols = st.columns([1,1])
        #         if bcols[0].button("✏️ Edit", key=f"edit_{row['id']}_{idx}"):
        #             st.session_state['edit_alert_id'] = row['id']
        #             st.rerun()
        #         if bcols[1].button("🗑️ Delete", key=f"delete_{row['id']}_{idx}"):
        #             mw.delete_alert(row['id'])
        #             st.warning("Alert deleted")
        #             st.rerun()

        # --- Alerts List in expander (replace your existing loop with this) ---
        with st.expander("Alerts List", expanded=True):
            if df_filtered.empty:
                st.info("No alerts to show.")
            else:
                # sort symbols by formatted label so groups appear alphabetically by display name
                symbols = sorted(df_filtered['symbol'].dropna().unique(), key=lambda s: format_sec_label(s).lower())

                for i, sym in enumerate(symbols):
                    group = df_filtered[df_filtered['symbol'] == sym]

                    # divider before each security except the first
                    if i > 0:
                        st.markdown("<hr style='border:none;border-top:1px solid #e6e6e6;margin:10px 0;'>", unsafe_allow_html=True)

                    # security heading (full width)
                    sec_label = format_sec_label(sym)
                    st.markdown(f"<h3 style='margin:6px 0 8px 0'>{sec_label}</h3>", unsafe_allow_html=True)

                    # render alerts for this security as indented rows using a small left column
                    for j, (_, row) in enumerate(group.iterrows()):
                        # parse params safely
                        params = json.loads(row["params"]) if isinstance(row["params"], str) else (row["params"] or {})
                        param_str = ", ".join(f"{k}={v}" for k, v in params.items()) if params else ""

                        # columns: tiny indent + content columns + action buttons
                        cols = st.columns([0.06, 2.6, 1.05, 1.0, 1.0, 1.0, 0.9, 0.6, 0.6])

                        # cols[0] is just the indent column (keeps things visually shifted right)
                        cols[1].markdown(f"**Type:** {row['alert_type']}")
                        cols[2].markdown(f"**Active:** {'✅' if row['active'] else '❌'}")
                        cols[3].markdown(f"**Notify:** {row['notify_mode']}")
                        cols[4].markdown(f"**Cooldown:** {row['cooldown_seconds']}s")
                        cols[5].markdown(f"**Last triggered:** {row['last_triggered'] or '—'}")
                        cols[6].markdown(f"**Note:** {row.get('note','—')}")

                        # parameters / thresholds on a second line (same indent)
                        if param_str:
                            # use the second column for params so it lines up under the type
                            cols[1].markdown(
                                f"<div style='margin-top:6px;color:#556;font-size:90%'><strong>Parameters:</strong> {param_str}</div>",
                                unsafe_allow_html=True
                            )

                        # action buttons: unique keys using alert id + group index + alert index
                        edit_key = f"edit_{row['id']}_{i}_{j}"
                        del_key = f"delete_{row['id']}_{i}_{j}"

                        if cols[7].button("✏️ Edit", key=edit_key):
                            st.session_state['edit_alert_id'] = int(row['id'])
                            st.rerun()

                        if cols[8].button("🗑️ Delete", key=del_key):
                            # immediate delete with spinner to reduce race clicks
                            with st.spinner("Deleting alert…"):
                                mw.delete_alert(int(row['id']))
                            st.warning(f"Alert {int(row['id'])} deleted.")
                            st.rerun()


        # --- Edit form (dynamic by alert type) ---
        if 'edit_alert_id' in st.session_state:
            edit_id = st.session_state['edit_alert_id']
            edit_row = df_filtered[df_filtered['id']==edit_id].iloc[0]
            st.markdown(f"### Edit Alert #{edit_id} — {format_sec_label(edit_row['symbol'])}")

            # Load existing parameters
            params = json.loads(edit_row['params']) if isinstance(edit_row['params'], str) else edit_row['params'] or {}

            with st.form(f"edit_alert_form_{edit_id}"):
                # Security & type display
                st.text_input("Security", value=format_sec_label(edit_row['symbol']), disabled=True)
                alert_type = edit_row['alert_type']
                st.text_input("Alert type", value=alert_type, disabled=True)

                # --- Parameters section ---
                st.markdown("#### Parameters")
                edited_params = {}

                if alert_type == "price":
                    mode_choice = st.radio("Threshold type", ["Absolute","Percentage"], index=0 if params.get("mode","absolute")=="absolute" else 1)
                    if mode_choice.lower() == "absolute":
                        tp = st.number_input("Target price", value=float(params.get("threshold",0.0)))
                        edited_params = {"threshold": float(tp), "mode":"absolute"}
                    else:
                        pct = st.number_input("Percentage of current price (%)", value=float(params.get("threshold",0.05)*100))
                        edited_params = {"threshold": float(pct)/100, "mode":"percentage"}
                    direction = st.selectbox("Direction", ["above","below"], index=0 if params.get("direction","above")=="above" else 1)
                    edited_params["direction"] = direction

                elif alert_type == "mos":
                    thr = st.number_input("MOS threshold (fraction)", value=float(params.get("threshold_pct",0.25)))
                    edited_params = {"threshold_pct": float(thr)}

                elif alert_type == "rsi":
                    window = st.number_input("RSI Window", value=int(params.get("window",14)))
                    overbought = st.number_input("Overbought threshold", value=float(params.get("overbought",70)))
                    underbought = st.number_input("Underbought threshold", value=float(params.get("underbought",30)))
                    trigger_on = st.selectbox("Trigger on", ["Cross above overbought","Cross below underbought","Both"],
                                              index=["Cross above overbought","Cross below underbought","Both"].index(params.get("trigger_on","Both")))
                    edited_params = {"window": int(window),"overbought": float(overbought),"underbought": float(underbought),"trigger_on": trigger_on}

                elif alert_type == "ma_crossover":
                    ma_choices = [5,10,20,50,100,200]
                    short_ma = st.selectbox("Short MA", ma_choices, index=ma_choices.index(int(params.get("short",20))))
                    long_ma = st.selectbox("Long MA", ma_choices, index=ma_choices.index(int(params.get("long",50))))
                    ma_type = st.selectbox("MA Type", ["SMA","EMA"], index=0 if params.get("ma_type","SMA")=="SMA" else 1)
                    crossover_type = st.selectbox("Crossover type", ["golden","death","both"],
                                                  index=["golden","death","both"].index(params.get("crossover_type","both")))
                    edited_params = {"short":int(short_ma),"long":int(long_ma),"ma_type":ma_type,"crossover_type":crossover_type}

                elif alert_type == "52w":
                    typ = st.selectbox("Check for", ["high","low"], index=0 if params.get("type","high")=="high" else 1)
                    edited_params = {"type": typ}

                elif alert_type == "volume_spike":
                    mult = st.number_input("Multiple vs avg", value=float(params.get("multiplier",2.0)))
                    look = st.number_input("Lookback days", value=int(params.get("lookback",20)))
                    edited_params = {"multiplier": float(mult), "lookback": int(look)}

                elif alert_type in ("dividend","earnings"):
                    days = st.number_input("Lookback days", value=int(params.get("lookback_days",7)))
                    edited_params = {"lookback_days": int(days)}

                # --- General settings ---
                st.markdown("#### General Settings")
                active_val = st.checkbox("Active", value=edit_row['active'])
                notify_val = st.selectbox("Notify mode", ["immediate","digest_hourly","digest_daily","digest_weekly"],
                                          index=["immediate","digest_hourly","digest_daily","digest_weekly"].index(edit_row['notify_mode']))
                cooldown_val = st.number_input("Cooldown seconds", value=int(edit_row['cooldown_seconds']))
                note_val = st.text_input("Note", value=edit_row.get('note',''))

                submitted = st.form_submit_button("Save changes")
                if submitted:
                    mw.edit_alert(
                        alert_id=edit_id,
                        active=active_val,
                        notify_mode=notify_val,
                        cooldown_seconds=int(cooldown_val),
                        note=note_val,
                        params=edited_params
                    )
                    st.success("✅ Alert updated")
                    del st.session_state['edit_alert_id']
                    st.rerun()


    # Create new alert in an expander
    with st.expander("➕ Create New Alert", expanded=False):

        # 1️⃣ Portfolio / Watchlist filter
        portfolio_options = ["Watchlist"] + [row["name"] for row in mw.list_portfolios().to_dict(orient="records")]
        selected_portfolios = st.multiselect(
            "Filter by portfolio / watchlist",
            options=portfolio_options,
            default=portfolio_options
        )

        # 2️⃣ Build symbol list with name (symbol)
        symbols = set()
        if "Watchlist" in selected_portfolios:
            symbols.update(mw.get_watchlist_symbols())
        for p in selected_portfolios:
            if p != "Watchlist":
                symbols.update(mw.get_portfolio_symbols(p))
        symbols = sorted(symbols)

        # Map symbol -> display name
        symbol_labels = [format_sec_label(sym) for sym in symbols]

        # 3️⃣ Symbol & alert type
        if not symbol_labels:
            st.info("No securities found. Add securities to your watchlist or portfolio first.")
            a_symbol = None
        else:
            a_symbol_label = st.selectbox("Security (name / symbol)", options=symbol_labels)
            a_symbol = a_symbol_label.split("(")[-1].replace(")", "").strip() if a_symbol_label else None

        a_type = st.selectbox(
            "Alert type",
            ["price","mos","rsi","ma_crossover","52w","volume_spike","dividend","earnings"],
            index=0
        )

        # --- Parameters section ---
        st.markdown("#### Parameters")
        params = {}

        if a_type == "price":
            price_mode = st.radio("Threshold type", ["Absolute", "Percentage"])
            if price_mode == "Absolute":
                tp = st.number_input("Target price", value=0.0)
                params = {"threshold": float(tp), "mode": "absolute"}
            else:
                pct = st.number_input("Percentage of current price (%)", value=5.0)
                params = {"threshold": float(pct)/100, "mode": "percentage"}
            direction = st.selectbox("Direction", ["above", "below"])
            params["direction"] = direction

        elif a_type == "mos":
            thr = st.number_input("MOS threshold (fraction)", value=0.25)
            params = {"threshold_pct": float(thr)}

        elif a_type == "rsi":
            window = st.number_input("RSI Window", value=14, help="Typical: 14 days")
            overbought = st.number_input("Overbought threshold", value=70)
            underbought = st.number_input("Underbought threshold", value=30)
            trigger_on = st.selectbox("Trigger on", ["Cross above overbought", "Cross below underbought", "Both"])
            params = {
                "window": int(window),
                "overbought": float(overbought),
                "underbought": float(underbought),
                "trigger_on": trigger_on
            }

        elif a_type == "ma_crossover":
            ma_choices = [5, 10, 20, 50, 100, 200]
            short_ma = st.selectbox("Short MA", ma_choices, index=2)
            long_ma = st.selectbox("Long MA", ma_choices, index=3)
            ma_type = st.selectbox("MA Type", ["SMA", "EMA"])
            crossover_type = st.selectbox("Crossover type", ["golden", "death", "both"])
            params = {
                "short": int(short_ma),
                "long": int(long_ma),
                "ma_type": ma_type,
                "crossover_type": crossover_type
            }

        elif a_type == "52w":
            typ = st.selectbox("Check for", ["high","low"])
            params = {"type": typ}

        elif a_type == "volume_spike":
            mult = st.number_input("Multiple vs avg", value=2.0)
            look = st.number_input("Lookback days", value=20)
            params = {"multiplier": float(mult), "lookback": int(look)}

        elif a_type in ("dividend","earnings"):
            days = st.number_input("Lookback days", value=7)
            params = {"lookback_days": int(days)}

        # --- General settings + submit ---
        st.markdown("#### General Settings")
        a_note = st.text_input("Note (optional)")

        with st.form("create_alert_submit"):
            a_notify = st.selectbox(
                "Notify mode",
                ["immediate","digest_hourly","digest_daily","digest_weekly"]
            )
            a_cool = st.number_input("Cooldown seconds", value=3600)

            submitted = st.form_submit_button("Create alert")
            if submitted:
                if not a_symbol:
                    st.error("No security selected — add securities to your watchlist or portfolio first.")
                else:
                    sec = mw.get_security(a_symbol)
                    if not sec:
                        st.error(f"Symbol {a_symbol} not found")
                    else:
                        sec_id = sec["id"]
                        mw.create_alert(
                            security_id=sec_id,
                            alert_type=a_type,
                            params=params,
                            notify_mode=a_notify,
                            cooldown_seconds=int(a_cool),
                            note=a_note
                        )
                        st.success("✅ Alert created")
                        st.rerun()


# ---------- SETTINGS ----------
with tabs[8]:
    st.header("Settings")
    st.info("All settings are stored in config.json. Edit below and save.")

    cfg = get_all_config()

    # -------------------
    # News
    # -------------------
    st.subheader("News settings")
    news_max = st.number_input(
        "News max items",
        value=int(cfg.get("news_max_items", 50)),
        min_value=1,
        key="cfg_news_max",
    )
    news_min_fetch = st.number_input(
        "News min fetch minutes (per symbol)",
        value=int(cfg.get("news_min_fetch_minutes", 30)),
        min_value=1,
        key="cfg_news_min_fetch",
    )

    # -------------------
    # Telegram
    # -------------------
    st.subheader("Telegram settings")
    t_token = st.text_input(
        "Telegram bot token",
        value=cfg.get("telegram_bot_token", ""),
        type="password",
        key="cfg_tele_token",
    )
    t_chat = st.text_input(
        "Telegram chat id",
        value=cfg.get("telegram_chat_id", ""),
        type="password",
        key="cfg_tele_chat",
    )

    # DND setting
    dnd_enabled = st.checkbox(
        "Enable DND (Do Not Disturb) — skip immediate alerts during DND",
        value=bool(cfg.get("dnd", False)),
        key="cfg_telegram_dnd",
    )

    # -------------------
    # Tax & Valuation
    # -------------------
    st.subheader("Tax & Valuation")
    tax_rate = st.number_input(
        "Tax rate (fraction)",
        value=float(cfg.get("tax_rate", 0.25)),
        step=0.01,
        key="cfg_tax_rate",
    )
    val_cache = st.number_input(
        "Valuation cache hours",
        value=int(cfg.get("valuation_cache_hours", 24)),
        min_value=1,
        key="cfg_val_cache",
    )
    kpi_cache = st.number_input(
        "KPI cache hours",
        value=int(cfg.get("kpi_cache_hours", 24)),
        min_value=1,
        key="cfg_kpi_cache",
    )

    # -------------------
    # Yahoo / Fetch
    # -------------------
    st.subheader("Yahoo / Fetch throttling")
    yf_max = st.number_input(
        "YF max req per minute",
        value=int(cfg.get("yf_max_req_per_min", 45)),
        min_value=1,
        key="cfg_yf_max",
    )
    yf_base_sleep = st.number_input(
        "YF base sleep sec",
        value=float(cfg.get("yf_base_sleep_sec", 0.8)),
        min_value=0.0,
        key="cfg_yf_sleep",
    )

    # -------------------
    # DCF Parameters
    # -------------------
    st.subheader("DCF Settings")
    dcf_years = st.number_input(
        "Projection years",
        value=int(cfg.get("dcf_projection_years", 10)),
        min_value=1,
        key="cfg_dcf_years",
    )
    dcf_discount = st.number_input(
        "Discount rate",
        value=float(cfg.get("dcf_discount_rate", 0.10)),
        step=0.01,
        key="cfg_dcf_discount",
    )
    dcf_growth = st.number_input(
        "Terminal growth",
        value=float(cfg.get("dcf_terminal_growth", 0.025)),
        step=0.001,
        key="cfg_dcf_growth",
    )
    dcf_conservative = st.checkbox(
        "Conservative mode",
        value=bool(cfg.get("dcf_conservative", True)),
        key="cfg_dcf_conservative",
    )

    # -------------------
    # Save
    # -------------------
    if st.button("Save settings", key="save_settings_btn"):
        set_config("news_max_items", int(news_max))
        set_config("news_min_fetch_minutes", int(news_min_fetch))
        set_config("telegram_bot_token", t_token)
        set_config("telegram_chat_id", t_chat)
        set_config("dnd", bool(dnd_enabled))
        set_config("tax_rate", float(tax_rate))
        set_config("valuation_cache_hours", int(val_cache))
        set_config("kpi_cache_hours", int(kpi_cache))
        set_config("yf_max_req_per_min", int(yf_max))
        set_config("yf_base_sleep_sec", float(yf_base_sleep))
        set_config("dcf_projection_years", int(dcf_years))
        set_config("dcf_discount_rate", float(dcf_discount))
        set_config("dcf_terminal_growth", float(dcf_growth))
        set_config("dcf_conservative", bool(dcf_conservative))

        st.success("Settings saved. Restart app for some changes to fully apply.")
