#!/usr/bin/env python3
"""
Visualization & Dashboard Generation
Zero Hunger Big Data Analytics Project

Generates:
  1. Matplotlib / Seaborn static charts (saved as PNG)
  2. Plotly interactive HTML dashboard
  3. Summary PDF report (optional – requires kaleido)

Charts produced:
  - Global trend lines per SDG indicator
  - Regional bar charts (top-N countries)
  - Choropleth world maps (Plotly)
  - Heatmaps (country × year for each indicator)
  - Scatter plots (cost of diet vs. unaffordability)
  - Feature importance bar charts (ML results)
  - Forecast line charts (actual vs. predicted)
  - Composite risk score map
"""

import os
import json
import argparse
import warnings

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")          # non-interactive backend for server / Colab
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.io as pio
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    warnings.warn("Plotly not installed – interactive charts will be skipped.")

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR    = os.environ.get("VIZ_OUTPUT_DIR", "output/visualizations")
PALETTE       = "viridis"
DIVERGING     = "RdYlGn_r"
FIGURE_DPI    = 150
FIGURE_SIZE   = (14, 7)

SDG_LABELS = {
    24001: "Prevalence of Undernourishment (%)",
    24003: "Prevalence of Food Insecurity (%)",
    24004: "Severely Food Insecure (thousands)",
    24005: "Mod/Severely Food Insecure (thousands)",
    7004:  "Cost of Healthy Diet (USD/day)",
    7005:  "Food Unaffordability (%)",
    7006:  "Number Unable to Afford Diet (millions)",
    7007:  "Cost of Starchy Staples (USD/day)",
}

sns.set_theme(style="whitegrid", palette=PALETTE)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _save_fig(fig: plt.Figure, name: str, subdir: str = "") -> str:
    _ensure_dir(os.path.join(OUTPUT_DIR, subdir))
    path = os.path.join(OUTPUT_DIR, subdir, f"{name}.png")
    fig.savefig(path, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"[save_fig] {path}")
    return path


def _save_html(fig, name: str, subdir: str = "") -> str:
    if not PLOTLY_AVAILABLE:
        return ""
    _ensure_dir(os.path.join(OUTPUT_DIR, subdir))
    path = os.path.join(OUTPUT_DIR, subdir, f"{name}.html")
    fig.write_html(path, include_plotlyjs="cdn")
    print(f"[save_html] {path}")
    return path


# =============================================================================
# 1. GLOBAL TREND CHARTS
# =============================================================================

def plot_global_trends(trend_df: pd.DataFrame,
                        item_codes: list | None = None) -> list[str]:
    """
    Line chart of global average per SDG indicator over time.

    Parameters
    ----------
    trend_df   : output of spark_aggregation.yearly_global_trend()
                 columns: year, item_code, global_avg
    item_codes : codes to plot (default: all available)

    Returns list of saved file paths.
    """
    if item_codes is None:
        item_codes = trend_df["item_code"].unique().tolist()

    saved = []
    for code in item_codes:
        subset = trend_df[trend_df["item_code"] == code].sort_values("year")
        if subset.empty:
            continue

        label = SDG_LABELS.get(int(code), f"Indicator {code}")
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        ax.plot(subset["year"], subset["global_avg"],
                marker="o", linewidth=2, color="#1f77b4", label="Global Avg")
        if "min_value" in subset.columns and "max_value" in subset.columns:
            ax.fill_between(subset["year"], subset["min_value"], subset["max_value"],
                             alpha=0.15, color="#1f77b4", label="Min–Max range")

        ax.set_title(f"Global Trend – {label}", fontsize=14, fontweight="bold")
        ax.set_xlabel("Year")
        ax.set_ylabel(label)
        ax.legend()
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        saved.append(_save_fig(fig, f"global_trend_{code}", "trends"))

    return saved


# =============================================================================
# 2. REGIONAL BAR CHARTS
# =============================================================================

def plot_regional_rankings(rank_df: pd.DataFrame,
                            item_codes: list | None = None,
                            top_n: int = 15) -> list[str]:
    """
    Horizontal bar charts showing top-N regions by average indicator value.

    Parameters
    ----------
    rank_df    : output of spark_aggregation.regional_rankings()
    item_codes : codes to plot
    top_n      : number of regions per chart
    """
    if item_codes is None:
        item_codes = rank_df["item_code"].unique().tolist()

    saved = []
    for code in item_codes:
        subset = (
            rank_df[rank_df["item_code"] == code]
            .nlargest(top_n, "avg_value")
        )
        if subset.empty:
            continue

        label = SDG_LABELS.get(int(code), f"Indicator {code}")
        fig, ax = plt.subplots(figsize=(12, 8))
        colors = plt.cm.RdYlGn_r(np.linspace(0.1, 0.9, len(subset)))
        bars = ax.barh(subset["area"], subset["avg_value"], color=colors)
        ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=8)
        ax.set_title(f"Top {top_n} Regions – {label}", fontsize=13, fontweight="bold")
        ax.set_xlabel(label)
        ax.invert_yaxis()
        saved.append(_save_fig(fig, f"ranking_{code}", "rankings"))

    return saved


# =============================================================================
# 3. HEATMAPS  (country × year)
# =============================================================================

def plot_heatmap(hunger_df: pd.DataFrame,
                 item_code: int,
                 top_n: int = 30) -> str:
    """
    Seaborn heatmap of indicator value per (country, year).

    Parameters
    ----------
    hunger_df  : long-format DataFrame with columns: area, year, item_code, value
    item_code  : SDG item code to visualise
    top_n      : limit to the top N countries by average value
    """
    subset = hunger_df[hunger_df["item_code"] == item_code].copy()
    if subset.empty:
        return ""

    top_areas = (
        subset.groupby("area")["value"].mean()
              .nlargest(top_n).index.tolist()
    )
    subset = subset[subset["area"].isin(top_areas)]
    pivot  = subset.pivot_table(index="area", columns="year", values="value",
                                 aggfunc="mean")

    label = SDG_LABELS.get(item_code, f"Indicator {item_code}")
    fig, ax = plt.subplots(figsize=(18, 10))
    sns.heatmap(pivot, cmap=DIVERGING, ax=ax, linewidths=0.3,
                linecolor="white", annot=False, fmt=".1f",
                cbar_kws={"label": label})
    ax.set_title(f"Heatmap – {label}\n(Top {top_n} most-affected regions)",
                  fontsize=13, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Region")
    plt.tight_layout()
    return _save_fig(fig, f"heatmap_{item_code}", "heatmaps")


# =============================================================================
# 4. SCATTER PLOTS
# =============================================================================

def plot_scatter_cost_vs_unaffordability(afford_df: pd.DataFrame,
                                          year: int | None = None) -> str:
    """
    Scatter: cost of healthy diet (x) vs. food unaffordability % (y).
    Bubble size encodes number of people unable to afford diet.
    """
    df = afford_df.copy()
    if year:
        df = df[df["year"] == year]
    df = df.dropna(subset=["healthy_diet_cost", "unaffordability_pct"])

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    size_col = "people_unable_to_afford_millions" if "people_unable_to_afford_millions" in df.columns else None
    sizes = (
        df[size_col].fillna(1).clip(1) * 5
        if size_col else np.full(len(df), 50)
    )
    scatter = ax.scatter(
        df["healthy_diet_cost"],
        df["unaffordability_pct"],
        s=sizes,
        c=df["unaffordability_pct"],
        cmap=DIVERGING,
        alpha=0.7,
        edgecolors="grey",
        linewidths=0.5,
    )
    plt.colorbar(scatter, ax=ax, label="Unaffordability %")
    title_year = f" ({year})" if year else ""
    ax.set_title(f"Cost of Healthy Diet vs. Food Unaffordability{title_year}",
                  fontsize=13, fontweight="bold")
    ax.set_xlabel("Cost of Healthy Diet (USD/day)")
    ax.set_ylabel("Prevalence of Food Unaffordability (%)")

    # Annotate notable outliers
    threshold = df["unaffordability_pct"].quantile(0.90)
    for _, row in df[df["unaffordability_pct"] >= threshold].iterrows():
        ax.annotate(row["area"], (row["healthy_diet_cost"], row["unaffordability_pct"]),
                    fontsize=7, alpha=0.8, ha="left")

    suffix = f"_{year}" if year else "_all_years"
    return _save_fig(fig, f"scatter_cost_unaffordability{suffix}", "scatter")


# =============================================================================
# 5. FEATURE IMPORTANCE
# =============================================================================

def plot_feature_importance(model_result: dict, model_name: str = "Model") -> str:
    """
    Horizontal bar chart of feature importances (RF / GBT) or
    absolute coefficients (LR).
    """
    if "feature_importances" in model_result:
        importances = model_result["feature_importances"]
        ylabel = "Feature Importance"
    elif "coefficients" in model_result:
        importances = {k: abs(v) for k, v in model_result["coefficients"].items()}
        ylabel = "Absolute Coefficient"
    else:
        return ""

    items  = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    feats, vals = zip(*items)

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(feats)))
    bars = ax.barh(feats, vals, color=colors)
    ax.bar_label(bars, fmt="%.4f", padding=3, fontsize=9)
    ax.set_title(f"{model_name} – Feature Importance", fontsize=13, fontweight="bold")
    ax.set_xlabel(ylabel)
    ax.invert_yaxis()
    return _save_fig(fig, f"feature_importance_{model_name.replace(' ', '_').lower()}",
                     "ml")


# =============================================================================
# 6. FORECAST CHARTS
# =============================================================================

def plot_forecast(historical_df: pd.DataFrame,
                  forecast_df: pd.DataFrame,
                  areas: list | None = None,
                  top_n: int = 10) -> list[str]:
    """
    Overlay historical Hunger Index values with model forecasts per country.

    Parameters
    ----------
    historical_df : DataFrame with columns Area, Year, Hunger_Index
    forecast_df   : output of ml_forecasting.forecast_hunger_index()
    areas         : specific areas to plot (default: top_n by avg hunger)
    top_n         : number of countries to plot if areas is None
    """
    if areas is None:
        areas = (
            historical_df.groupby("Area")["Hunger_Index"]
                         .mean()
                         .nlargest(top_n)
                         .index.tolist()
        )

    saved = []
    for area in areas:
        hist = historical_df[historical_df["Area"] == area].sort_values("Year")
        fore = forecast_df[forecast_df["Area"] == area].sort_values("Year")
        if hist.empty and fore.empty:
            continue

        fig, ax = plt.subplots(figsize=(12, 5))
        if not hist.empty and "Hunger_Index" in hist.columns:
            ax.plot(hist["Year"], hist["Hunger_Index"],
                    marker="o", label="Historical", linewidth=2, color="#1f77b4")
        if not fore.empty:
            ax.plot(fore["Year"], fore["Predicted_Hunger_Index"],
                    marker="s", linestyle="--", label="Forecast",
                    linewidth=2, color="#d62728")
            # Confidence band (±RMSE)
            if "Model_RMSE" in fore.columns:
                rmse = fore["Model_RMSE"].iloc[0]
                ax.fill_between(
                    fore["Year"],
                    fore["Predicted_Hunger_Index"] - rmse,
                    fore["Predicted_Hunger_Index"] + rmse,
                    alpha=0.2, color="#d62728", label=f"±{rmse:.2f} RMSE",
                )

        ax.set_title(f"Hunger Index Forecast – {area}", fontsize=13, fontweight="bold")
        ax.set_xlabel("Year")
        ax.set_ylabel("Hunger Index")
        ax.legend()
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        saved.append(_save_fig(fig, f"forecast_{area.replace(' ', '_')}", "forecasts"))

    return saved


# =============================================================================
# 7. PLOTLY INTERACTIVE DASHBOARD
# =============================================================================

def build_interactive_dashboard(trend_df: pd.DataFrame,
                                  rank_df: pd.DataFrame,
                                  afford_df: pd.DataFrame,
                                  risk_df: pd.DataFrame,
                                  forecast_df: pd.DataFrame | None = None) -> str:
    """
    Build a multi-tab Plotly HTML dashboard.

    Tabs:
      1. Global Trends
      2. Regional Rankings
      3. Affordability
      4. Risk Scores
      5. Forecast (optional)

    Returns path to saved HTML file.
    """
    if not PLOTLY_AVAILABLE:
        print("[dashboard] Plotly not available – skipping.")
        return ""

    # ── Tab 1: Global Trends ──────────────────────────────────────────────
    fig_trend = px.line(
        trend_df.sort_values("year"),
        x="year", y="global_avg", color="item_code",
        labels={"year": "Year", "global_avg": "Global Average", "item_code": "Indicator"},
        title="Global Trends per SDG Food Security Indicator",
        template="plotly_white",
    )
    fig_trend.update_traces(mode="lines+markers")

    # ── Tab 2: Regional Rankings ──────────────────────────────────────────
    latest_rank = (
        rank_df.groupby(["area", "item_code"])["avg_value"].mean().reset_index()
    )
    fig_rank = px.bar(
        latest_rank.groupby("item_code").apply(
            lambda g: g.nlargest(15, "avg_value"), include_groups=False
        ).reset_index(drop=True),
        x="avg_value", y="area", color="item_code",
        barmode="group", orientation="h",
        title="Top 15 Regions by Average Indicator Value",
        labels={"avg_value": "Average Value", "area": "Region"},
        template="plotly_white",
    )
    fig_rank.update_layout(yaxis={"categoryorder": "total ascending"})

    # ── Tab 3: Affordability Scatter ──────────────────────────────────────
    afford_latest = afford_df[
        afford_df["year"] == afford_df["year"].max()
    ].dropna(subset=["healthy_diet_cost", "unaffordability_pct"])

    size_col = (
        "people_unable_to_afford_millions"
        if "people_unable_to_afford_millions" in afford_latest.columns else None
    )
    fig_afford = px.scatter(
        afford_latest,
        x="healthy_diet_cost",
        y="unaffordability_pct",
        size=size_col or None,
        color="unaffordability_pct",
        hover_name="area",
        color_continuous_scale="RdYlGn_r",
        title=f"Cost of Healthy Diet vs. Food Unaffordability ({afford_latest['year'].iloc[0]})",
        labels={"healthy_diet_cost": "Cost (USD/day)",
                "unaffordability_pct": "Unaffordability (%)"},
        template="plotly_white",
    )

    # ── Tab 4: Composite Risk Choropleth ──────────────────────────────────
    risk_latest = risk_df[risk_df["year"] == risk_df["year"].max()]
    fig_map = px.choropleth(
        risk_latest,
        locations="area",
        locationmode="country names",
        color="composite_risk",
        color_continuous_scale="RdYlGn_r",
        title=f"Composite Food Security Risk Score ({risk_latest['year'].iloc[0] if not risk_latest.empty else 'Latest'})",
        labels={"composite_risk": "Risk Score (0–1)"},
        template="plotly_white",
    )
    fig_map.update_layout(geo=dict(showframe=False, showcoastlines=True))

    # ── Combine into subplots dashboard ──────────────────────────────────
    dashboard = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "Global Trends",
            "Regional Rankings (Top 15)",
            "Diet Cost vs. Unaffordability",
            "Composite Risk Map",
        ],
        specs=[
            [{"type": "xy"},    {"type": "xy"}],
            [{"type": "xy"},    {"type": "choropleth"}],
        ],
    )

    for trace in fig_trend.data:
        dashboard.add_trace(trace, row=1, col=1)
    for trace in fig_rank.data:
        dashboard.add_trace(trace, row=1, col=2)
    for trace in fig_afford.data:
        dashboard.add_trace(trace, row=2, col=1)
    for trace in fig_map.data:
        dashboard.add_trace(trace, row=2, col=2)

    dashboard.update_layout(
        height=900,
        title_text="Zero Hunger Big Data Analytics – Dashboard",
        title_font_size=18,
        showlegend=False,
        template="plotly_white",
    )

    path = _save_html(dashboard, "zero_hunger_dashboard", "")

    # Also save individual charts
    _save_html(fig_trend,  "global_trends_interactive",    "interactive")
    _save_html(fig_rank,   "regional_rankings_interactive", "interactive")
    _save_html(fig_afford, "affordability_interactive",     "interactive")
    _save_html(fig_map,    "risk_map_interactive",          "interactive")

    # ── Tab 5: Forecast (if available) ───────────────────────────────────
    if forecast_df is not None and not forecast_df.empty:
        fig_fore = px.line(
            forecast_df.sort_values("Year"),
            x="Year", y="Predicted_Hunger_Index", color="Area",
            title="Hunger Index Forecast (Next 3 Years)",
            labels={"Predicted_Hunger_Index": "Predicted Hunger Index"},
            template="plotly_white",
        )
        _save_html(fig_fore, "forecast_interactive", "interactive")

    return path


# =============================================================================
# 8. CORRELATION HEATMAP
# =============================================================================

def plot_correlation_matrix(df: pd.DataFrame,
                              cols: list | None = None) -> str:
    """
    Seaborn correlation heatmap for the feature matrix.
    """
    if cols is None:
        cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cols = [c for c in cols if c in df.columns]
    if len(cols) < 2:
        return ""

    corr = df[cols].corr()
    mask = np.triu(np.ones_like(corr, dtype=bool))

    fig, ax = plt.subplots(figsize=(12, 10))
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f",
                cmap="coolwarm", center=0, ax=ax,
                linewidths=0.5, annot_kws={"size": 9})
    ax.set_title("Feature Correlation Matrix", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return _save_fig(fig, "correlation_matrix", "ml")


# =============================================================================
# 9. GROWTH RATE CHART
# =============================================================================

def plot_growth_rates(growth_df: pd.DataFrame,
                       item_codes: list | None = None,
                       top_n: int = 15) -> list[str]:
    """
    Bar chart of % growth for top-N countries per indicator.
    Red = increase (worsening), green = decrease (improvement).
    """
    if item_codes is None:
        item_codes = growth_df["item_code"].unique().tolist()

    saved = []
    for code in item_codes:
        subset = growth_df[growth_df["item_code"] == code].dropna(subset=["pct_change"])
        if subset.empty:
            continue

        top    = subset.nlargest(top_n, "pct_change")
        label  = SDG_LABELS.get(int(code), f"Indicator {code}")
        colors = ["#d62728" if v > 0 else "#2ca02c" for v in top["pct_change"]]

        fig, ax = plt.subplots(figsize=(12, 7))
        bars = ax.barh(top["area"], top["pct_change"], color=colors)
        ax.bar_label(bars, fmt="%.1f%%", padding=3, fontsize=8)
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_title(f"Growth Rate (start → end) – {label}", fontsize=13, fontweight="bold")
        ax.set_xlabel("% Change")
        ax.invert_yaxis()
        saved.append(_save_fig(fig, f"growth_rate_{code}", "growth"))

    return saved


# =============================================================================
# Full visualisation pipeline
# =============================================================================

def run_visualization_pipeline(
    trend_csv:    str,
    rank_csv:     str,
    afford_csv:   str,
    risk_csv:     str,
    growth_csv:   str,
    hunger_long_csv: str,
    forecast_csv: str | None = None,
    feature_csv:  str | None = None,
    ml_results_json: str | None = None,
) -> None:
    """
    End-to-end visualisation pipeline.

    All CSV inputs are expected to be outputs of spark_aggregation.py
    and ml_forecasting.py.
    """
    _ensure_dir(OUTPUT_DIR)

    print("=" * 60)
    print("LOADING DATA")
    print("=" * 60)
    trend_df   = pd.read_csv(trend_csv)
    rank_df    = pd.read_csv(rank_csv)
    afford_df  = pd.read_csv(afford_csv)
    risk_df    = pd.read_csv(risk_csv)
    growth_df  = pd.read_csv(growth_csv)
    hunger_df  = pd.read_csv(hunger_long_csv)
    forecast_df = pd.read_csv(forecast_csv) if forecast_csv else None
    feature_df  = pd.read_csv(feature_csv)  if feature_csv  else None
    ml_results  = None
    if ml_results_json and os.path.exists(ml_results_json):
        with open(ml_results_json) as f:
            ml_results = json.load(f)

    print("\nSTEP 1 – Global trend charts")
    plot_global_trends(trend_df)

    print("STEP 2 – Regional ranking charts")
    plot_regional_rankings(rank_df)

    print("STEP 3 – Heatmaps")
    for code in [24001, 24003, 24004]:
        if code in hunger_df.get("item_code", pd.Series([], dtype=int)).values:
            plot_heatmap(hunger_df, code)

    print("STEP 4 – Scatter: cost vs. unaffordability")
    plot_scatter_cost_vs_unaffordability(afford_df)

    print("STEP 5 – Growth rate charts")
    plot_growth_rates(growth_df)

    print("STEP 6 – Correlation matrix")
    if feature_df is not None:
        plot_correlation_matrix(feature_df)

    print("STEP 7 – Feature importance")
    if ml_results:
        plot_feature_importance(ml_results, model_name=ml_results.get("name", "Model"))

    print("STEP 8 – Forecast charts")
    if forecast_df is not None and feature_df is not None and "Hunger_Index" in feature_df.columns:
        plot_forecast(feature_df, forecast_df)

    print("STEP 9 – Interactive HTML dashboard")
    build_interactive_dashboard(trend_df, rank_df, afford_df, risk_df, forecast_df)

    print(f"\nAll visualisations saved → {OUTPUT_DIR}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Zero Hunger pipeline – visualisation & dashboard")
    parser.add_argument("--trend",      required=True, help="yearly_global_trend CSV")
    parser.add_argument("--rank",       required=True, help="regional_rankings CSV")
    parser.add_argument("--afford",     required=True, help="affordability_metrics CSV")
    parser.add_argument("--risk",       required=True, help="composite_risk_scores CSV")
    parser.add_argument("--growth",     required=True, help="growth_rate_analysis CSV")
    parser.add_argument("--hunger-long", required=True,
                        help="Long-format hunger indicators CSV (area,year,item_code,value)")
    parser.add_argument("--forecast",   default=None, help="hunger_index_forecast CSV")
    parser.add_argument("--features",   default=None, help="Preprocessed feature CSV")
    parser.add_argument("--ml-results", default=None,
                        help="JSON file with ML model metrics/importances")
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    args = parser.parse_args()

    OUTPUT_DIR = args.output_dir
    run_visualization_pipeline(
        trend_csv=args.trend,
        rank_csv=args.rank,
        afford_csv=args.afford,
        risk_csv=args.risk,
        growth_csv=args.growth,
        hunger_long_csv=args.hunger_long,
        forecast_csv=args.forecast,
        feature_csv=args.features,
        ml_results_json=args.ml_results,
    )
