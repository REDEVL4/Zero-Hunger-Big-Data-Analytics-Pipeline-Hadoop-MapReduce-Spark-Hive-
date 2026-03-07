#!/usr/bin/env python3
"""
Machine Learning Forecasting Module
Zero Hunger Big Data Analytics Project

Models implemented:
  1. Linear Regression  (PySpark MLlib)
  2. Random Forest      (PySpark MLlib)
  3. Gradient-Boosted Trees (PySpark MLlib)
  4. scikit-learn versions for local / notebook use

Evaluations:
  - RMSE, MAE, R²
  - Feature importance (RF / GBT)
  - 3-year ahead forecasting
  - Cross-validation (k-fold)
"""

import os
import json
import argparse
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# PySpark ML (used when a SparkSession is available)
# ---------------------------------------------------------------------------
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.ml import Pipeline
    from pyspark.ml.regression import (
        LinearRegression,
        RandomForestRegressor,
        GBTRegressor,
    )
    from pyspark.ml.feature import VectorAssembler, StandardScaler as SparkStandardScaler
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# ---------------------------------------------------------------------------
# scikit-learn (always available, used as fallback and for local runs)
# ---------------------------------------------------------------------------
from sklearn.linear_model import LinearRegression as SKLinearRegression
from sklearn.ensemble import RandomForestRegressor as SKRandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------------------------
# Feature / target definitions
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    "Agricultural_Yield",
    "Malnutrition_Rate",
    "GDP_Spending",
]

# Additional features used when available
EXTENDED_FEATURE_COLS = FEATURE_COLS + [
    "Cost_Healthy_Diet",
    "Food_Unaffordability_Pct",
    "Prevalence_Undernourishment",
]

TARGET_COL  = "Hunger_Index"
HORIZON     = 3          # years ahead for forecasting
OUTPUT_DIR  = os.environ.get("ML_OUTPUT_DIR", "output/ml")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _save_metrics(metrics: dict, name: str) -> None:
    _ensure_dir(OUTPUT_DIR)
    path = os.path.join(OUTPUT_DIR, f"{name}_metrics.json")
    with open(path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"[save_metrics] {path}")


# ---------------------------------------------------------------------------
# PySpark ML pipeline
# ---------------------------------------------------------------------------

def get_spark(app_name: str = "ZeroHungerML") -> "SparkSession":
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def build_spark_pipeline(feature_cols: list,
                          regressor,
                          scale: bool = True) -> "Pipeline":
    """Assemble a VectorAssembler + optional StandardScaler + regressor Pipeline."""
    stages = []
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features",
                                 handleInvalid="skip")
    stages.append(assembler)

    if scale:
        scaler = SparkStandardScaler(inputCol="raw_features", outputCol="features",
                                      withMean=True, withStd=True)
        stages.append(scaler)
        final_features = "features"
    else:
        # Rename so all downstream stages use "features"
        from pyspark.ml.feature import SQLTransformer
        stages.append(
            SQLTransformer(statement="SELECT *, raw_features AS features FROM __THIS__")
        )
        final_features = "features"

    regressor = regressor.setFeaturesCol("features").setLabelCol(TARGET_COL)
    stages.append(regressor)
    return Pipeline(stages=stages)


def evaluate_spark_model(predictions: "DataFrame",
                          label_col: str = TARGET_COL) -> dict:
    """Return RMSE, MAE, and R² for a Spark predictions DataFrame."""
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction")
    rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    mae  = evaluator.setMetricName("mae").evaluate(predictions)
    r2   = evaluator.setMetricName("r2").evaluate(predictions)
    return {"rmse": round(rmse, 4), "mae": round(mae, 4), "r2": round(r2, 4)}


def train_spark_linear_regression(spark_df: "DataFrame",
                                   feature_cols: list | None = None,
                                   test_size: float = 0.2) -> dict:
    """
    Train a PySpark Linear Regression model with cross-validation.

    Returns a dict with model, metrics, and feature importance proxy.
    """
    if feature_cols is None:
        feature_cols = [c for c in EXTENDED_FEATURE_COLS if c in spark_df.columns]

    spark_df = spark_df.dropna(subset=feature_cols + [TARGET_COL])
    train_df, test_df = spark_df.randomSplit([1 - test_size, test_size], seed=42)

    lr = LinearRegression(maxIter=200, regParam=0.01, elasticNetParam=0.8,
                           solver="auto")
    pipeline = build_spark_pipeline(feature_cols, lr)

    # Hyper-parameter grid search via CrossValidator
    param_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.001, 0.01, 0.1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .build()
    )
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=RegressionEvaluator(labelCol=TARGET_COL, metricName="rmse"),
        numFolds=5,
        seed=42,
    )

    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel
    predictions = best_model.transform(test_df)

    metrics = evaluate_spark_model(predictions)
    print(f"[LR-Spark] RMSE={metrics['rmse']}  MAE={metrics['mae']}  R²={metrics['r2']}")
    _save_metrics(metrics, "spark_linear_regression")

    # Coefficients (proxy for feature importance)
    lr_model = best_model.stages[-1]
    coef_dict = {feat: round(float(coef), 6)
                 for feat, coef in zip(feature_cols, lr_model.coefficients)}

    return {
        "model": best_model,
        "metrics": metrics,
        "coefficients": coef_dict,
        "best_params": {
            "regParam": lr_model.getRegParam(),
            "elasticNetParam": lr_model.getElasticNetParam(),
        },
    }


def train_spark_random_forest(spark_df: "DataFrame",
                               feature_cols: list | None = None,
                               test_size: float = 0.2,
                               num_trees: int = 100) -> dict:
    """
    Train a PySpark Random Forest regressor.

    Returns model, metrics, and feature importances.
    """
    if feature_cols is None:
        feature_cols = [c for c in EXTENDED_FEATURE_COLS if c in spark_df.columns]

    spark_df = spark_df.dropna(subset=feature_cols + [TARGET_COL])
    train_df, test_df = spark_df.randomSplit([1 - test_size, test_size], seed=42)

    rf = RandomForestRegressor(
        numTrees=num_trees,
        maxDepth=6,
        featureSubsetStrategy="auto",
        seed=42,
    )
    pipeline = build_spark_pipeline(feature_cols, rf, scale=False)

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100, 200])
        .addGrid(rf.maxDepth, [4, 6, 8])
        .build()
    )
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=RegressionEvaluator(labelCol=TARGET_COL, metricName="rmse"),
        numFolds=5,
        seed=42,
    )

    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel
    predictions = best_model.transform(test_df)

    metrics = evaluate_spark_model(predictions)
    print(f"[RF-Spark] RMSE={metrics['rmse']}  MAE={metrics['mae']}  R²={metrics['r2']}")
    _save_metrics(metrics, "spark_random_forest")

    rf_model = best_model.stages[-1]
    importances = dict(zip(feature_cols, [round(float(x), 6)
                                          for x in rf_model.featureImportances]))

    return {
        "model": best_model,
        "metrics": metrics,
        "feature_importances": importances,
    }


# ---------------------------------------------------------------------------
# scikit-learn ML pipeline (local / notebook)
# ---------------------------------------------------------------------------

def _prepare_sklearn_data(df: pd.DataFrame,
                           feature_cols: list | None = None,
                           test_size: float = 0.2):
    """Return X_train, X_test, y_train, y_test from a pandas DataFrame."""
    if feature_cols is None:
        feature_cols = [c for c in EXTENDED_FEATURE_COLS if c in df.columns]

    available = [c for c in feature_cols if c in df.columns]
    df_clean = df[available + [TARGET_COL]].dropna()

    X = df_clean[available].values
    y = df_clean[TARGET_COL].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42
    )
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test  = scaler.transform(X_test)
    return X_train, X_test, y_train, y_test, available, scaler


def sklearn_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae  = float(mean_absolute_error(y_true, y_pred))
    r2   = float(r2_score(y_true, y_pred))
    return {"rmse": round(rmse, 4), "mae": round(mae, 4), "r2": round(r2, 4)}


def train_sklearn_linear_regression(df: pd.DataFrame,
                                     feature_cols: list | None = None) -> dict:
    """
    scikit-learn Linear Regression with 5-fold cross-validation.

    Returns model, metrics, and coefficients.
    """
    X_train, X_test, y_train, y_test, cols, scaler = _prepare_sklearn_data(
        df, feature_cols)

    model = SKLinearRegression()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    metrics = sklearn_metrics(y_test, y_pred)

    kf  = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_rmse = np.sqrt(-cross_val_score(
        SKLinearRegression(), X_train, y_train,
        cv=kf, scoring="neg_mean_squared_error"
    ))
    metrics["cv_rmse_mean"] = round(float(cv_rmse.mean()), 4)
    metrics["cv_rmse_std"]  = round(float(cv_rmse.std()), 4)

    print(f"[LR-sklearn] RMSE={metrics['rmse']}  MAE={metrics['mae']}  "
          f"R²={metrics['r2']}  CV-RMSE={metrics['cv_rmse_mean']}±{metrics['cv_rmse_std']}")
    _save_metrics(metrics, "sklearn_linear_regression")

    coef_dict = {feat: round(float(c), 6) for feat, c in zip(cols, model.coef_)}
    return {
        "model": model,
        "scaler": scaler,
        "features": cols,
        "metrics": metrics,
        "coefficients": coef_dict,
        "intercept": round(float(model.intercept_), 6),
    }


def train_sklearn_random_forest(df: pd.DataFrame,
                                 feature_cols: list | None = None,
                                 n_estimators: int = 200) -> dict:
    """
    scikit-learn Random Forest regressor with 5-fold CV.

    Returns model, metrics, and feature importances.
    """
    X_train, X_test, y_train, y_test, cols, scaler = _prepare_sklearn_data(
        df, feature_cols)

    model = SKRandomForestRegressor(
        n_estimators=n_estimators,
        max_depth=8,
        min_samples_split=4,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    metrics = sklearn_metrics(y_test, y_pred)

    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_rmse = np.sqrt(-cross_val_score(
        SKRandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1),
        X_train, y_train, cv=kf, scoring="neg_mean_squared_error"
    ))
    metrics["cv_rmse_mean"] = round(float(cv_rmse.mean()), 4)
    metrics["cv_rmse_std"]  = round(float(cv_rmse.std()), 4)

    print(f"[RF-sklearn] RMSE={metrics['rmse']}  MAE={metrics['mae']}  "
          f"R²={metrics['r2']}  CV-RMSE={metrics['cv_rmse_mean']}±{metrics['cv_rmse_std']}")
    _save_metrics(metrics, "sklearn_random_forest")

    importances = {feat: round(float(imp), 6)
                   for feat, imp in zip(cols, model.feature_importances_)}
    return {
        "model": model,
        "scaler": scaler,
        "features": cols,
        "metrics": metrics,
        "feature_importances": importances,
    }


def train_sklearn_gradient_boosting(df: pd.DataFrame,
                                     feature_cols: list | None = None) -> dict:
    """Gradient Boosted Trees regressor (scikit-learn)."""
    X_train, X_test, y_train, y_test, cols, scaler = _prepare_sklearn_data(
        df, feature_cols)

    model = GradientBoostingRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=4,
        subsample=0.8,
        random_state=42,
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    metrics = sklearn_metrics(y_test, y_pred)
    print(f"[GBT-sklearn] RMSE={metrics['rmse']}  MAE={metrics['mae']}  R²={metrics['r2']}")
    _save_metrics(metrics, "sklearn_gradient_boosting")

    importances = {feat: round(float(imp), 6)
                   for feat, imp in zip(cols, model.feature_importances_)}
    return {
        "model": model,
        "scaler": scaler,
        "features": cols,
        "metrics": metrics,
        "feature_importances": importances,
    }


# ---------------------------------------------------------------------------
# Forecasting (out-of-sample projection)
# ---------------------------------------------------------------------------

def forecast_hunger_index(df: pd.DataFrame,
                           model_result: dict,
                           horizon: int = HORIZON) -> pd.DataFrame:
    """
    Project the Hunger Index `horizon` years into the future for each country.

    Uses the last known feature values and extrapolates using linear trend
    for each feature, then applies the trained model.

    Parameters
    ----------
    df           : cleaned feature DataFrame (must include 'Area', 'Year',
                   feature columns, and TARGET_COL)
    model_result : dict returned by train_sklearn_* functions
    horizon      : number of years to forecast

    Returns
    -------
    DataFrame with columns: Area, Year, Predicted_Hunger_Index, model_name
    """
    model   = model_result["model"]
    scaler  = model_result["scaler"]
    cols    = model_result["features"]
    metrics = model_result["metrics"]

    # For each country, compute linear trend slope per feature
    latest_year = df["Year"].max()
    records = []

    for area, grp in df.groupby("Area"):
        grp = grp.sort_values("Year")
        if len(grp) < 2 or not all(c in grp.columns for c in cols):
            continue

        # Fit a simple linear trend for each feature over time
        slopes = {}
        last_vals = {}
        for c in cols:
            valid = grp[["Year", c]].dropna()
            if len(valid) < 2:
                slopes[c]    = 0.0
                last_vals[c] = float(grp[c].dropna().iloc[-1]) if not grp[c].dropna().empty else 0.0
                continue
            coefs = np.polyfit(valid["Year"], valid[c], 1)
            slopes[c]    = float(coefs[0])
            last_vals[c] = float(valid[c].iloc[-1])

        for h in range(1, horizon + 1):
            future_year = int(latest_year) + h
            # NOTE: extrapolation assumes linear feature trends; predictions are
            # clamped to [0, 100] to prevent physiologically impossible values.
            feat_vals = np.array([
                last_vals[c] + slopes[c] * h for c in cols
            ]).reshape(1, -1)
            feat_scaled = scaler.transform(feat_vals)
            pred = float(model.predict(feat_scaled)[0])
            pred = max(0.0, min(100.0, pred))   # clamp to valid hunger-index range
            records.append({
                "Area":                   area,
                "Year":                   future_year,
                "Predicted_Hunger_Index": round(pred, 4),
                "Model_R2":               metrics["r2"],
                "Model_RMSE":             metrics["rmse"],
            })

    forecast_df = pd.DataFrame(records)
    _ensure_dir(OUTPUT_DIR)
    out_path = os.path.join(OUTPUT_DIR, "hunger_index_forecast.csv")
    forecast_df.to_csv(out_path, index=False)
    print(f"[forecast] Saved {len(forecast_df)} rows → {out_path}")
    return forecast_df


# ---------------------------------------------------------------------------
# Compare all models
# ---------------------------------------------------------------------------

def compare_models(df: pd.DataFrame,
                   feature_cols: list | None = None) -> pd.DataFrame:
    """
    Train LR, RF, and GBT models and return a comparison DataFrame.

    Parameters
    ----------
    df           : cleaned feature DataFrame
    feature_cols : list of feature column names (auto-detected if None)

    Returns
    -------
    DataFrame with columns: Model, RMSE, MAE, R², CV_RMSE_mean
    """
    results = []

    for name, train_fn in [
        ("Linear Regression",    train_sklearn_linear_regression),
        ("Random Forest",        train_sklearn_random_forest),
        ("Gradient Boosting",    train_sklearn_gradient_boosting),
    ]:
        try:
            res = train_fn(df, feature_cols)
            m = res["metrics"]
            results.append({
                "Model":       name,
                "RMSE":        m.get("rmse"),
                "MAE":         m.get("mae"),
                "R2":          m.get("r2"),
                "CV_RMSE":     m.get("cv_rmse_mean"),
            })
        except Exception as exc:
            print(f"[compare_models] {name} failed: {exc}")

    comparison = pd.DataFrame(results).sort_values("RMSE")
    _ensure_dir(OUTPUT_DIR)
    comparison.to_csv(os.path.join(OUTPUT_DIR, "model_comparison.csv"), index=False)
    print("\nModel comparison:")
    print(comparison.to_string(index=False))
    return comparison


# ---------------------------------------------------------------------------
# Full ML pipeline entry point
# ---------------------------------------------------------------------------

def run_ml_pipeline(feature_csv: str,
                    feature_cols: list | None = None,
                    horizon: int = HORIZON) -> None:
    """
    End-to-end ML pipeline.

    1. Load preprocessed feature CSV.
    2. Train and evaluate all three models.
    3. Select best model by RMSE.
    4. Forecast `horizon` years ahead.
    5. Save all results.
    """
    print("=" * 60)
    print("LOADING FEATURES")
    print("=" * 60)
    df = pd.read_csv(feature_csv)
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")

    if TARGET_COL not in df.columns:
        raise ValueError(
            f"Target column '{TARGET_COL}' not found in the feature file. "
            f"Available columns: {list(df.columns)}"
        )

    print("\n" + "=" * 60)
    print("TRAINING MODELS")
    print("=" * 60)
    comparison = compare_models(df, feature_cols)

    # Retrain best model for forecasting
    best_model_name = comparison.iloc[0]["Model"]
    train_fn_map = {
        "Linear Regression": train_sklearn_linear_regression,
        "Random Forest":     train_sklearn_random_forest,
        "Gradient Boosting": train_sklearn_gradient_boosting,
    }
    best_result = train_fn_map[best_model_name](df, feature_cols)
    print(f"\nBest model: {best_model_name}")

    print("\n" + "=" * 60)
    print(f"FORECASTING  (+{horizon} years)")
    print("=" * 60)
    forecast_df = forecast_hunger_index(df, best_result, horizon=horizon)
    print(forecast_df.head(10).to_string(index=False))

    print("\nML pipeline complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Zero Hunger pipeline – ML forecasting")
    parser.add_argument("--features",  required=True,
                        help="Path to preprocessed feature CSV")
    parser.add_argument("--horizon",   type=int, default=HORIZON,
                        help="Forecast horizon in years")
    parser.add_argument("--output-dir", default=OUTPUT_DIR,
                        help="Directory for model metrics and forecast CSV")
    args = parser.parse_args()

    OUTPUT_DIR = args.output_dir
    run_ml_pipeline(feature_csv=args.features, horizon=args.horizon)
