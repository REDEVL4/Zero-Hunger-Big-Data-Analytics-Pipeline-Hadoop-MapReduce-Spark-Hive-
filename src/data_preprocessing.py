#!/usr/bin/env python3
"""
Data Preprocessing & Cleaning Pipeline
Zero Hunger Big Data Analytics Project

Handles:
- Loading raw data from FAO / World Bank / UNICEF sources
- Missing value imputation
- Outlier detection and treatment
- Feature normalization and standardization
- Dataset merging and export
"""

import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.impute import SimpleImputer, KNNImputer


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SDG_ITEM_CODES = {
    24001: "Prevalence of undernourishment",
    24003: "Prevalence of moderate or severe food insecurity",
    24004: "Number of people affected by severe food insecurity",
    24005: "Number of people moderately or severely food insecure",
    7004:  "Cost of healthy diet",
    7005:  "Prevalence of food unaffordability",
    7006:  "Number unable to afford healthy diet",
    7007:  "Cost of starchy staples",
}

NUMERIC_FEATURES = [
    "Agricultural_Yield",
    "Malnutrition_Rate",
    "GDP_Spending",
    "Hunger_Index",
    "Value",
]

RAW_DATA_DIR = os.environ.get("RAW_DATA_DIR", "data/raw")
PROCESSED_DATA_DIR = os.environ.get("PROCESSED_DATA_DIR", "data/processed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_sdg_bulk(filepath: str) -> pd.DataFrame:
    """Load and do basic typing on the FAO SDG bulk download CSV."""
    df = pd.read_csv(filepath, low_memory=False)
    df.columns = df.columns.str.strip()

    # Standardise column names that vary across FAO exports
    rename_map = {
        "Area Code": "AreaCode",
        "Area Code (M49)": "AreaCode",
        "Area": "Area",
        "Item Code": "ItemCode",
        "Item Code (CPC)": "ItemCode",
        "Item": "Item",
        "Year": "Year",
        "Unit": "Unit",
        "Value": "Value",
        "Flag": "Flag",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns},
              inplace=True)

    # Coerce types
    for col in ("AreaCode", "ItemCode", "Year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "Value" in df.columns:
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

    return df


def load_hunger_index(filepath: str) -> pd.DataFrame:
    """Load the interpolated Global Hunger Index dataset."""
    if filepath.endswith(".xlsx"):
        df = pd.read_excel(filepath)
    else:
        df = pd.read_csv(filepath, low_memory=False)
    df.columns = df.columns.str.strip()
    return df


def load_faostat(filepath: str) -> pd.DataFrame:
    """Load FAOSTAT agriculture/production data."""
    df = pd.read_csv(filepath, low_memory=False)
    df.columns = df.columns.str.strip()
    if "Value" in df.columns:
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def filter_sdg_indicators(df: pd.DataFrame,
                           item_codes: list | None = None) -> pd.DataFrame:
    """Keep only rows whose ItemCode matches SDG food-security codes."""
    if item_codes is None:
        item_codes = list(SDG_ITEM_CODES.keys())
    if "ItemCode" not in df.columns:
        return df
    mask = df["ItemCode"].isin(item_codes)
    filtered = df[mask].copy()
    print(f"[filter_sdg_indicators] {len(df):,} → {len(filtered):,} rows kept")
    return filtered


# ---------------------------------------------------------------------------
# Missing-value handling
# ---------------------------------------------------------------------------

def report_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame summarising missing values per column."""
    missing = df.isnull().sum()
    pct = missing / len(df) * 100
    return pd.DataFrame({"Missing": missing, "Pct": pct.round(2)}).query("Missing > 0")


def impute_numeric(df: pd.DataFrame,
                   strategy: str = "median",
                   knn: bool = False) -> pd.DataFrame:
    """
    Impute missing values in numeric columns.

    Parameters
    ----------
    df : pd.DataFrame
    strategy : str  – 'mean', 'median', or 'most_frequent'  (used when knn=False)
    knn : bool      – use k-NN imputation (better for correlated features)
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        return df

    if knn:
        imputer = KNNImputer(n_neighbors=5)
    else:
        imputer = SimpleImputer(strategy=strategy)

    df = df.copy()
    df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
    return df


def fill_time_series_gaps(df: pd.DataFrame,
                           group_cols: list,
                           value_col: str = "Value") -> pd.DataFrame:
    """
    Forward-fill then backward-fill a time-series value within groups.
    Useful for country × indicator time series with sparse years.
    """
    df = df.sort_values(group_cols + ["Year"]).copy()
    df[value_col] = (
        df.groupby(group_cols)[value_col]
          .transform(lambda s: s.ffill().bfill())
    )
    return df


# ---------------------------------------------------------------------------
# Outlier detection and treatment
# ---------------------------------------------------------------------------

def remove_outliers_iqr(df: pd.DataFrame,
                         col: str,
                         factor: float = 3.0) -> pd.DataFrame:
    """
    Cap outliers using the IQR method (Tukey fences with configurable factor).
    Values outside [Q1 - factor*IQR, Q3 + factor*IQR] are clipped to the fence.
    """
    if col not in df.columns:
        return df
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    df = df.copy()
    clipped = ((df[col] < lower) | (df[col] > upper)).sum()
    df[col] = df[col].clip(lower, upper)
    if clipped:
        print(f"[remove_outliers_iqr] '{col}': clipped {clipped} outliers "
              f"to [{lower:.3f}, {upper:.3f}]")
    return df


def flag_outliers_zscore(df: pd.DataFrame,
                          col: str,
                          threshold: float = 3.5) -> pd.DataFrame:
    """Add a boolean column '<col>_outlier' flagging z-score outliers."""
    if col not in df.columns:
        return df
    df = df.copy()
    z = (df[col] - df[col].mean()) / df[col].std(ddof=0)
    df[f"{col}_outlier"] = z.abs() > threshold
    n = df[f"{col}_outlier"].sum()
    print(f"[flag_outliers_zscore] '{col}': {n} outliers flagged (|z| > {threshold})")
    return df


# ---------------------------------------------------------------------------
# Normalisation / Standardisation
# ---------------------------------------------------------------------------

def normalize_minmax(df: pd.DataFrame,
                     cols: list | None = None,
                     feature_range: tuple = (0, 1)) -> tuple[pd.DataFrame, MinMaxScaler]:
    """
    Min-Max scale selected columns to *feature_range*.

    Returns the transformed DataFrame and the fitted scaler so that
    it can be persisted and later used for inverse transform.
    """
    if cols is None:
        cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cols = [c for c in cols if c in df.columns]
    scaler = MinMaxScaler(feature_range=feature_range)
    df = df.copy()
    df[cols] = scaler.fit_transform(df[cols])
    return df, scaler


def standardize_zscore(df: pd.DataFrame,
                        cols: list | None = None) -> tuple[pd.DataFrame, StandardScaler]:
    """
    Z-score standardise selected columns (mean=0, std=1).

    Returns the transformed DataFrame and the fitted scaler.
    """
    if cols is None:
        cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cols = [c for c in cols if c in df.columns]
    scaler = StandardScaler()
    df = df.copy()
    df[cols] = scaler.fit_transform(df[cols])
    return df, scaler


# ---------------------------------------------------------------------------
# Dataset merging
# ---------------------------------------------------------------------------

def merge_sdg_with_hunger_index(sdg_df: pd.DataFrame,
                                 hunger_df: pd.DataFrame,
                                 on: list | None = None) -> pd.DataFrame:
    """
    Merge the filtered SDG dataset with the Global Hunger Index dataset.
    Performs a left join on Area + Year (column names are normalised first).
    """
    if on is None:
        on = ["Area", "Year"]

    # Normalise column names in hunger_df
    col_map = {c: c.strip() for c in hunger_df.columns}
    hunger_df = hunger_df.rename(columns=col_map)
    for alt in ("Country", "country", "Nation", "nation"):
        if alt in hunger_df.columns and "Area" not in hunger_df.columns:
            hunger_df = hunger_df.rename(columns={alt: "Area"})
    if "Year" not in hunger_df.columns:
        for alt in ("year", "DATE", "date"):
            if alt in hunger_df.columns:
                hunger_df = hunger_df.rename(columns={alt: "Year"})

    merged = sdg_df.merge(hunger_df, on=on, how="left", suffixes=("", "_hunger"))
    print(f"[merge] SDG={len(sdg_df):,}  Hunger={len(hunger_df):,}  "
          f"Merged={len(merged):,}")
    return merged


def build_feature_matrix(sdg_df: pd.DataFrame,
                          faostat_df: pd.DataFrame,
                          hunger_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot SDG indicators into wide format and merge with hunger index and
    FAOSTAT agricultural yield, creating the feature matrix used by ML models.

    Returns a tidy DataFrame with one row per (Area, Year).
    """
    # --- Pivot SDG indicators to wide format ---
    sdg_pivot = (
        sdg_df[sdg_df["ItemCode"].isin(SDG_ITEM_CODES)]
        .pivot_table(index=["Area", "AreaCode", "Year"],
                     columns="ItemCode",
                     values="Value",
                     aggfunc="mean")
        .reset_index()
    )
    sdg_pivot.columns.name = None
    # After pivot, numeric ItemCode values become column names.
    # Build the rename map using only the integer keys that are present.
    rename_map = {
        code: label.replace(" ", "_")
        for code, label in SDG_ITEM_CODES.items()
        if code in sdg_pivot.columns
    }
    sdg_pivot.rename(columns=rename_map, inplace=True)

    # --- Aggregate FAOSTAT: agricultural yield per country/year ---
    ag_yield = (
        faostat_df.groupby(["Area", "Year"])["Value"]
        .mean()
        .reset_index()
        .rename(columns={"Value": "Agricultural_Yield"})
    )

    # --- Merge everything ---
    df = sdg_pivot.merge(ag_yield, on=["Area", "Year"], how="left")

    # Normalise Area/Year column names in hunger_df then merge
    for alt in ("Country", "country", "Nation"):
        if alt in hunger_df.columns:
            hunger_df = hunger_df.rename(columns={alt: "Area"})
    if "GHI Score" in hunger_df.columns:
        hunger_df = hunger_df.rename(columns={"GHI Score": "Hunger_Index"})
    elif "ghi" in hunger_df.columns:
        hunger_df = hunger_df.rename(columns={"ghi": "Hunger_Index"})

    if "Hunger_Index" in hunger_df.columns:
        df = df.merge(hunger_df[["Area", "Year", "Hunger_Index"]],
                      on=["Area", "Year"], how="left")

    return df


# ---------------------------------------------------------------------------
# Full preprocessing pipeline
# ---------------------------------------------------------------------------

def preprocess_pipeline(sdg_path: str,
                         faostat_path: str,
                         hunger_index_path: str,
                         output_path: str | None = None,
                         normalize: bool = True,
                         standardize: bool = False) -> pd.DataFrame:
    """
    End-to-end preprocessing pipeline.

    Parameters
    ----------
    sdg_path          : path to FAO SDG bulk download CSV
    faostat_path      : path to FAOSTAT crop/production CSV
    hunger_index_path : path to interpolated hunger index CSV or XLSX
    output_path       : if provided, save cleaned CSV here
    normalize         : apply Min-Max normalisation to numeric features
    standardize       : apply Z-score standardisation instead of Min-Max

    Returns
    -------
    Cleaned, merged, and (optionally) scaled DataFrame ready for ML/Spark.
    """
    print("=" * 60)
    print("STEP 1 – Loading raw data")
    print("=" * 60)
    sdg_df    = load_sdg_bulk(sdg_path)
    faostat   = load_faostat(faostat_path)
    hunger_df = load_hunger_index(hunger_index_path)

    print(f"  SDG rows: {len(sdg_df):,}")
    print(f"  FAOSTAT rows: {len(faostat):,}")
    print(f"  Hunger index rows: {len(hunger_df):,}")

    print("\nSTEP 2 – Filtering SDG indicators")
    sdg_df = filter_sdg_indicators(sdg_df)

    print("\nSTEP 3 – Reporting missing values")
    for label, df in [("SDG", sdg_df), ("FAOSTAT", faostat), ("Hunger", hunger_df)]:
        mv = report_missing(df)
        if mv.empty:
            print(f"  {label}: no missing values")
        else:
            print(f"  {label}:\n{mv.to_string()}\n")

    print("\nSTEP 4 – Imputing missing values")
    sdg_df  = impute_numeric(sdg_df, strategy="median")
    faostat = impute_numeric(faostat, strategy="median")
    hunger_df = impute_numeric(hunger_df, strategy="median")

    print("\nSTEP 5 – Filling time-series gaps")
    if "ItemCode" in sdg_df.columns and "AreaCode" in sdg_df.columns:
        sdg_df = fill_time_series_gaps(sdg_df, ["AreaCode", "ItemCode"])

    print("\nSTEP 6 – Outlier treatment (IQR capping)")
    sdg_df = remove_outliers_iqr(sdg_df, "Value")
    if "Value" in faostat.columns:
        faostat = remove_outliers_iqr(faostat, "Value")

    print("\nSTEP 7 – Building feature matrix")
    feature_df = build_feature_matrix(sdg_df, faostat, hunger_df)
    print(f"  Feature matrix shape: {feature_df.shape}")

    print("\nSTEP 8 – Final imputation on feature matrix")
    feature_df = impute_numeric(feature_df, strategy="median")

    print("\nSTEP 9 – Scaling")
    if standardize:
        numeric_cols = feature_df.select_dtypes(include=[np.number]).columns.difference(
            ["AreaCode", "Year"]).tolist()
        feature_df, scaler = standardize_zscore(feature_df, cols=numeric_cols)
        print(f"  Z-score standardised {len(numeric_cols)} columns")
    elif normalize:
        numeric_cols = feature_df.select_dtypes(include=[np.number]).columns.difference(
            ["AreaCode", "Year"]).tolist()
        feature_df, scaler = normalize_minmax(feature_df, cols=numeric_cols)
        print(f"  Min-Max normalised {len(numeric_cols)} columns")

    print("\nSTEP 10 – Saving output")
    if output_path:
        _ensure_dir(os.path.dirname(output_path))
        feature_df.to_csv(output_path, index=False)
        print(f"  Saved → {output_path}")

    print("\nPreprocessing complete.")
    print(f"Final shape: {feature_df.shape}")
    return feature_df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Zero Hunger pipeline – data preprocessing")
    parser.add_argument("--sdg",     required=True,
                        help="Path to FAO SDG bulk download CSV")
    parser.add_argument("--faostat", required=True,
                        help="Path to FAOSTAT CSV")
    parser.add_argument("--hunger",  required=True,
                        help="Path to hunger index CSV or XLSX")
    parser.add_argument("--output",  default="data/processed/features.csv",
                        help="Output CSV path")
    parser.add_argument("--normalize",   action="store_true", default=True)
    parser.add_argument("--standardize", action="store_true", default=False)
    args = parser.parse_args()

    preprocess_pipeline(
        sdg_path=args.sdg,
        faostat_path=args.faostat,
        hunger_index_path=args.hunger,
        output_path=args.output,
        normalize=args.normalize,
        standardize=args.standardize,
    )
