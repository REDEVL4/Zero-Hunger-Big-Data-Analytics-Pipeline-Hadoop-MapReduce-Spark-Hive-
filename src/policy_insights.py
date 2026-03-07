#!/usr/bin/env python3
"""
Policy Insights Extraction & Reporting
Zero Hunger Big Data Analytics Project

Generates:
  1. Hotspot identification (countries / regions requiring urgent attention)
  2. Trend-based risk classification (improving / stable / worsening)
  3. Priority ranking by composite burden score
  4. Targeted policy recommendations per region
  5. Markdown and plain-text summary reports
  6. Machine-readable JSON output for downstream systems
"""

import os
import json
import argparse
from datetime import date

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = os.environ.get("INSIGHTS_OUTPUT_DIR", "output/policy_insights")

# Thresholds based on FAO / WHO guidelines
THRESHOLDS = {
    "undernourishment_pct":     {"low": 5,   "medium": 15,  "high": 25},
    "food_insecurity_pct":      {"low": 10,  "medium": 25,  "high": 40},
    "unaffordability_pct":      {"low": 10,  "medium": 30,  "high": 60},
    "composite_risk":           {"low": 0.2, "medium": 0.5, "high": 0.7},
    "cost_gap":                 {"low": 0.5, "medium": 1.5, "high": 3.0},
}

POLICY_LIBRARY = {
    "social_protection": (
        "Expand social protection programmes (cash transfers, school meals, food vouchers) "
        "targeting the poorest quintile. Evidence shows that well-designed transfers reduce "
        "child stunting by up to 12% and improve diet diversity."
    ),
    "agricultural_investment": (
        "Increase public investment in smallholder agriculture, irrigation infrastructure, "
        "and drought-resistant crop varieties. A 1% increase in agricultural GDP growth "
        "reduces the poverty headcount by 3–6x more than growth in other sectors in low-income countries."
    ),
    "market_integration": (
        "Reduce post-harvest losses and improve market integration through rural road "
        "construction, cold-chain logistics, and digital commodity exchanges. "
        "Post-harvest losses account for 30–40% of food production in sub-Saharan Africa."
    ),
    "nutrition_education": (
        "Implement community-based nutrition education focusing on dietary diversity, "
        "infant and young child feeding, and micronutrient supplementation. "
        "Behaviour-change communication programmes can reduce stunting by 5–10 pp within 3 years."
    ),
    "emergency_response": (
        "Pre-position emergency food reserves and activate humanitarian response mechanisms "
        "for countries in crisis (IPC Phase 3+). Early warning systems reduce emergency "
        "response costs by up to 40%."
    ),
    "trade_policy": (
        "Negotiate preferential trade agreements and reduce tariff barriers on nutritious "
        "foods (fruits, vegetables, legumes). Import diversification insulates domestic "
        "markets from global commodity price shocks."
    ),
    "climate_resilience": (
        "Integrate climate-smart agriculture practices – agroforestry, conservation "
        "agriculture, and water harvesting – to build resilience to extreme weather events "
        "that are increasingly disrupting food production."
    ),
    "gender_equity": (
        "Close the gender gap in agricultural productivity by ensuring equal access to land, "
        "credit, extension services, and inputs for women farmers. "
        "Closing the gender gap could increase agricultural output by 2.5–4%."
    ),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _classify(value: float, thresholds: dict) -> str:
    """Classify a value as 'low', 'medium', 'high', or 'critical'."""
    if pd.isna(value):
        return "unknown"
    if value >= thresholds["high"]:
        return "critical"
    if value >= thresholds["medium"]:
        return "high"
    if value >= thresholds["low"]:
        return "medium"
    return "low"


def _trend_label(pct_change: float) -> str:
    """Convert a % change to a human-readable trend label."""
    if pd.isna(pct_change):
        return "unknown"
    if pct_change >  15:
        return "rapidly_worsening"
    if pct_change >   5:
        return "worsening"
    if pct_change >= -5:
        return "stable"
    if pct_change >= -15:
        return "improving"
    return "rapidly_improving"


# ---------------------------------------------------------------------------
# Hotspot identification
# ---------------------------------------------------------------------------

def identify_hotspots(risk_df: pd.DataFrame,
                       year: int | None = None,
                       top_n: int = 30) -> pd.DataFrame:
    """
    Identify food insecurity hotspots based on composite risk score.

    Parameters
    ----------
    risk_df : DataFrame with columns area, year, composite_risk,
              undernourishment_pct, insecurity_pct
    year    : target year (default: latest)
    top_n   : number of hotspot countries to return

    Returns
    -------
    DataFrame with hotspot classification and recommended priority tier.
    """
    if year is None:
        year = int(risk_df["year"].max())

    subset = risk_df[risk_df["year"] == year].copy()
    if subset.empty:
        return pd.DataFrame()

    subset["risk_class"] = subset["composite_risk"].apply(
        lambda v: _classify(v, THRESHOLDS["composite_risk"])
    )

    subset["priority_tier"] = pd.cut(
        subset["composite_risk"],
        bins=[-np.inf, 0.2, 0.4, 0.6, 0.8, np.inf],
        labels=["Tier 5 – Low", "Tier 4 – Watch",
                "Tier 3 – Concern", "Tier 2 – High", "Tier 1 – Critical"],
    )

    return (
        subset
        .nlargest(top_n, "composite_risk")
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Trend classification
# ---------------------------------------------------------------------------

def classify_trends(growth_df: pd.DataFrame,
                    item_code: int = 24001) -> pd.DataFrame:
    """
    Classify each country's indicator trend as improving / stable / worsening.

    Parameters
    ----------
    growth_df  : output of spark_aggregation.growth_rate_analysis()
    item_code  : SDG code to analyse

    Returns
    -------
    DataFrame with area, pct_change, trend_label, urgency
    """
    subset = growth_df[growth_df["item_code"] == item_code].copy()
    subset["trend_label"] = subset["pct_change"].apply(_trend_label)
    subset["urgency"] = subset["trend_label"].map({
        "rapidly_worsening": "Critical",
        "worsening":         "High",
        "stable":            "Medium",
        "improving":         "Low",
        "rapidly_improving": "Monitor",
        "unknown":           "Unknown",
    })
    return subset.sort_values("pct_change", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Burden scoring
# ---------------------------------------------------------------------------

def compute_burden_scores(risk_df: pd.DataFrame,
                           afford_df: pd.DataFrame,
                           year: int | None = None) -> pd.DataFrame:
    """
    Composite burden score combining:
      - Composite risk (normalised)
      - Cost gap (healthy diet vs. staples, normalised)
      - Number of people unable to afford diet

    Higher score = heavier burden = higher policy priority.
    """
    if year is None:
        year = int(risk_df["year"].max())

    r = risk_df[risk_df["year"] == year][["area", "composite_risk"]].copy()
    a = afford_df[afford_df["year"] == year].copy()

    merged = r.merge(a, on="area", how="left")

    for col, alias in [("composite_risk", "risk_norm"),
                        ("cost_gap",       "cost_norm"),
                        ("people_unable_to_afford_millions", "people_norm")]:
        if col in merged.columns:
            col_min = merged[col].min()
            col_max = merged[col].max()
            if col_max > col_min:
                merged[alias] = (merged[col] - col_min) / (col_max - col_min)
            else:
                merged[alias] = 0.0
        else:
            merged[alias] = 0.0

    merged["burden_score"] = (
        0.4 * merged["risk_norm"] +
        0.3 * merged["cost_norm"] +
        0.3 * merged["people_norm"]
    ).round(4)

    return (
        merged[["area", "composite_risk", "cost_gap",
                "people_unable_to_afford_millions",
                "risk_norm", "cost_norm", "people_norm", "burden_score"]]
        .sort_values("burden_score", ascending=False)
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Policy recommendations
# ---------------------------------------------------------------------------

def recommend_policies(row: pd.Series) -> list[str]:
    """
    Select relevant policy actions for a given country row based on its
    indicator profile and trend.

    Returns a list of policy keys from POLICY_LIBRARY.
    """
    recs = []

    risk  = row.get("composite_risk", 0) or 0
    trend = row.get("trend_label", "stable")

    if risk >= THRESHOLDS["composite_risk"]["high"]:
        recs.append("emergency_response")
    if trend in ("rapidly_worsening", "worsening"):
        recs.append("social_protection")
    recs.append("agricultural_investment")

    unafford = row.get("unaffordability_pct", 0) or 0
    if unafford >= THRESHOLDS["unaffordability_pct"]["medium"]:
        recs.append("market_integration")
        recs.append("trade_policy")

    if risk >= THRESHOLDS["composite_risk"]["medium"]:
        recs.append("nutrition_education")

    recs.append("climate_resilience")
    recs.append("gender_equity")

    # De-duplicate preserving order
    seen, unique = set(), []
    for r in recs:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return unique


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_hotspot_report(hotspots: pd.DataFrame,
                              trends: pd.DataFrame,
                              burden: pd.DataFrame,
                              forecast_df: pd.DataFrame | None = None,
                              year: int | None = None) -> str:
    """
    Generate a Markdown policy brief.

    Returns the Markdown text and saves it to OUTPUT_DIR.
    """
    _ensure_dir(OUTPUT_DIR)
    if year is None and not hotspots.empty:
        year = int(hotspots["year"].iloc[0]) if "year" in hotspots.columns else date.today().year

    lines = [
        "# Zero Hunger Pipeline – Policy Insights Report",
        f"**Generated:** {date.today().isoformat()}  |  **Reference Year:** {year}",
        "",
        "---",
        "",
        "## 1. Executive Summary",
        "",
        (
            "This report synthesises outputs from the Big Data Analytics Pipeline "
            "(Hadoop MapReduce, Apache Spark, Hive) applied to FAO, World Bank, and "
            "UNICEF datasets covering 180+ countries from 2000 to 2023.  "
            "The analysis identifies food insecurity hotspots, classifies country trends, "
            "and translates data insights into actionable policy recommendations aligned "
            "with **UN SDG 2 – Zero Hunger**."
        ),
        "",
        "---",
        "",
        "## 2. Hotspot Identification (Top 20 Priority Countries)",
        "",
        "Countries are ranked by composite risk score (0–1; higher = worse).",
        "",
    ]

    if not hotspots.empty:
        display_cols = [c for c in ["area", "year", "composite_risk",
                                     "undernourishment_pct", "insecurity_pct",
                                     "priority_tier", "risk_class"]
                        if c in hotspots.columns]
        lines.append(hotspots[display_cols].head(20).to_markdown(index=False))
    else:
        lines.append("_No hotspot data available._")

    lines += [
        "",
        "---",
        "",
        "## 3. Trend Classification",
        "",
        "Countries are grouped by the direction and speed of change in "
        "Prevalence of Undernourishment (SDG Indicator 24001).",
        "",
    ]

    if not trends.empty:
        for status in ["rapidly_worsening", "worsening", "stable",
                        "improving", "rapidly_improving"]:
            subset = trends[trends["trend_label"] == status]
            if subset.empty:
                continue
            label = status.replace("_", " ").title()
            lines.append(f"### {label} ({len(subset)} countries)")
            lines.append(", ".join(subset["area"].head(15).tolist()))
            lines.append("")

    lines += [
        "---",
        "",
        "## 4. Burden Scoring & Priority Ranking",
        "",
        "Composite burden score combines risk, cost gap, and population affected.",
        "",
    ]

    if not burden.empty:
        display_cols = [c for c in ["area", "composite_risk", "cost_gap",
                                     "people_unable_to_afford_millions", "burden_score"]
                        if c in burden.columns]
        lines.append(burden[display_cols].head(20).to_markdown(index=False))

    lines += [
        "",
        "---",
        "",
        "## 5. Policy Recommendations",
        "",
        "The following recommendations are prioritised for Tier 1 – Critical countries.",
        "",
    ]

    if not hotspots.empty and not burden.empty:
        if "priority_tier" in hotspots.columns:
            critical = hotspots[
                hotspots["priority_tier"].astype(str).str.contains("Critical", na=False)
            ].head(10)
        else:
            # Fall back to top 10 by composite risk if priority_tier column is absent
            critical = hotspots.nlargest(10, "composite_risk") if "composite_risk" in hotspots.columns else hotspots.head(10)

        for _, row in critical.iterrows():
            area  = row.get("area", "Unknown")
            score = row.get("composite_risk", "N/A")
            trend_row = trends[trends["area"] == area].iloc[0] if not trends[trends["area"] == area].empty else pd.Series()
            row_merged = row.copy()
            if not trend_row.empty:
                row_merged = pd.concat([row_merged, trend_row], axis=0).drop_duplicates()

            rec_keys = recommend_policies(row_merged)
            lines.append(f"### {area}  (Risk score: {score:.3f})")
            lines.append("")
            for key in rec_keys:
                lines.append(f"- **{key.replace('_', ' ').title()}**")
                lines.append(f"  {POLICY_LIBRARY[key]}")
                lines.append("")

    if forecast_df is not None and not forecast_df.empty:
        lines += [
            "---",
            "",
            "## 6. Forecasting Outlook",
            "",
            "Predicted Hunger Index for the next 3 years (top 15 countries):",
            "",
        ]
        top_forecast = (
            forecast_df
            .groupby("Area")["Predicted_Hunger_Index"].mean()
            .nlargest(15).reset_index()
            .rename(columns={"Predicted_Hunger_Index": "Avg_Forecast_HI"})
        )
        lines.append(top_forecast.to_markdown(index=False))

    lines += [
        "",
        "---",
        "",
        "## 7. Methodology Notes",
        "",
        "| Component | Technology | Details |",
        "|-----------|-----------|---------|",
        "| Data ingestion | HDFS 3.3.6 | 86,657 records, ~62 MB |",
        "| Data processing | Hadoop MapReduce (Python streaming) | 5 jobs |",
        "| Aggregations | Apache Spark 3.5.3 | 6 aggregation pipelines |",
        "| SQL analysis | Apache Hive 4.0.1 | 30+ queries across 7 SDG indicators |",
        "| ML forecasting | scikit-learn / PySpark MLlib | LR, RF, GBT |",
        "| Visualisation | Matplotlib, Seaborn, Plotly | 20+ chart types |",
        "",
        "---",
        "",
        "_Report generated by the Zero Hunger Big Data Analytics Pipeline._",
    ]

    md_text = "\n".join(lines)
    md_path = os.path.join(OUTPUT_DIR, "policy_insights_report.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_text)
    print(f"[report] Markdown report saved → {md_path}")
    return md_text


def export_json_insights(hotspots: pd.DataFrame,
                          trends: pd.DataFrame,
                          burden: pd.DataFrame) -> str:
    """
    Export machine-readable JSON with hotspot data and recommendations.
    """
    _ensure_dir(OUTPUT_DIR)
    insights = {
        "generated": date.today().isoformat(),
        "hotspots":  [],
        "trend_summary": {},
        "top_burden": [],
    }

    for _, row in hotspots.head(30).iterrows():
        trend_row = trends[trends["area"] == row.get("area", "")].iloc[0] \
                    if not trends[trends["area"] == row.get("area", "")].empty else pd.Series()
        row_merged = row.copy()
        if not trend_row.empty:
            row_merged = pd.concat([row_merged, trend_row]).drop_duplicates()
        recs = recommend_policies(row_merged)
        entry = {
            "area":            row.get("area"),
            "year":            int(row.get("year", 0)) if not pd.isna(row.get("year", 0)) else None,
            "composite_risk":  round(float(row["composite_risk"]), 4) if "composite_risk" in row else None,
            "priority_tier":   str(row.get("priority_tier", "")),
            "risk_class":      str(row.get("risk_class", "")),
            "recommendations": recs,
        }
        insights["hotspots"].append(entry)

    if not trends.empty and "trend_label" in trends.columns:
        for label, grp in trends.groupby("trend_label"):
            insights["trend_summary"][label] = grp["area"].tolist()

    for _, row in burden.head(20).iterrows():
        insights["top_burden"].append({
            "area":         row.get("area"),
            "burden_score": round(float(row.get("burden_score", 0)), 4),
        })

    json_path = os.path.join(OUTPUT_DIR, "policy_insights.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(insights, f, indent=2, ensure_ascii=False)
    print(f"[export] JSON insights saved → {json_path}")
    return json_path


# ---------------------------------------------------------------------------
# Full policy insights pipeline
# ---------------------------------------------------------------------------

def run_policy_insights_pipeline(
    risk_csv:    str,
    growth_csv:  str,
    afford_csv:  str,
    forecast_csv: str | None = None,
    year: int | None = None,
) -> None:
    """
    End-to-end policy insights pipeline.

    Reads aggregation CSVs, computes insights, and writes reports.
    """
    print("=" * 60)
    print("LOADING AGGREGATION OUTPUTS")
    print("=" * 60)
    risk_df   = pd.read_csv(risk_csv)
    growth_df = pd.read_csv(growth_csv)
    afford_df = pd.read_csv(afford_csv)
    forecast_df = pd.read_csv(forecast_csv) if forecast_csv else None

    if year is None:
        year = int(risk_df["year"].max()) if "year" in risk_df.columns else None

    print(f"  Reference year: {year}")

    print("\nSTEP 1 – Identify hotspots")
    hotspots = identify_hotspots(risk_df, year=year)
    print(f"  {len(hotspots)} hotspot countries identified")

    print("\nSTEP 2 – Classify trends")
    trends = classify_trends(growth_df, item_code=24001)
    print(trends["trend_label"].value_counts().to_string())

    print("\nSTEP 3 – Compute burden scores")
    burden = compute_burden_scores(risk_df, afford_df, year=year)

    print("\nSTEP 4 – Generate Markdown report")
    generate_hotspot_report(hotspots, trends, burden, forecast_df, year=year)

    print("\nSTEP 5 – Export JSON insights")
    export_json_insights(hotspots, trends, burden)

    print("\nPolicy insights pipeline complete.")
    print(f"Outputs → {OUTPUT_DIR}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Zero Hunger pipeline – policy insights")
    parser.add_argument("--risk",       required=True, help="composite_risk_scores CSV")
    parser.add_argument("--growth",     required=True, help="growth_rate_analysis CSV")
    parser.add_argument("--afford",     required=True, help="affordability_metrics CSV")
    parser.add_argument("--forecast",   default=None,  help="hunger_index_forecast CSV")
    parser.add_argument("--year",       type=int, default=None,
                        help="Reference year for analysis (default: latest in data)")
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    args = parser.parse_args()

    OUTPUT_DIR = args.output_dir
    run_policy_insights_pipeline(
        risk_csv=args.risk,
        growth_csv=args.growth,
        afford_csv=args.afford,
        forecast_csv=args.forecast,
        year=args.year,
    )
