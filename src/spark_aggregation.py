#!/usr/bin/env python3
"""
Complete Spark Aggregation Pipeline
Zero Hunger Big Data Analytics Project

Aggregations produced:
  1. Yearly global trend per SDG indicator
  2. Regional averages and rankings
  3. Growth-rate analysis
  4. Cost-of-diet affordability metrics
  5. Cross-indicator composite risk scores
  6. Outputs saved as Parquet + CSV for downstream ML / visualisation
"""

import os
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SDG_ITEM_CODES = {
    24001: "Prevalence_Undernourishment",
    24003: "Prevalence_Food_Insecurity",
    24004: "Number_Severely_Food_Insecure",
    24005: "Number_Mod_Severely_Food_Insecure",
    7004:  "Cost_Healthy_Diet",
    7005:  "Food_Unaffordability_Pct",
    7006:  "Number_Unable_Afford_Diet",
    7007:  "Cost_Starchy_Staples",
}

HDFS_OUTPUT_BASE = os.environ.get("HDFS_OUTPUT_BASE", "/output")
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "output/spark")


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark(app_name: str = "ZeroHungerAggregation") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

HUNGER_SCHEMA = StructType([
    StructField("year",          IntegerType(), True),
    StructField("area_code",     IntegerType(), True),
    StructField("area",          StringType(),  True),
    StructField("item_code",     IntegerType(), True),
    StructField("item_code_sdg", StringType(),  True),
    StructField("item",          StringType(),  True),
    StructField("value",         DoubleType(),  True),
    StructField("unit",          StringType(),  True),
    StructField("avg_value",     DoubleType(),  True),
    StructField("growth_rate",   DoubleType(),  True),
])

AFFORDABILITY_SCHEMA = StructType([
    StructField("area",     StringType(), True),
    StructField("year",     IntegerType(), True),
    StructField("cost_usd", DoubleType(),  True),
])


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_mapreduce_output(spark: SparkSession,
                          path: str,
                          schema: StructType) -> DataFrame:
    """Load tab-separated MapReduce output from HDFS or local FS."""
    df = (
        spark.read
        .schema(schema)
        .option("sep", "\t")
        .option("header", "false")
        .csv(path)
    )
    print(f"[load] {path}  →  {df.count():,} rows")
    return df


def load_feature_csv(spark: SparkSession, path: str) -> DataFrame:
    """Load the preprocessed feature CSV produced by data_preprocessing.py."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


# ---------------------------------------------------------------------------
# Core aggregation functions
# ---------------------------------------------------------------------------

def yearly_global_trend(df: DataFrame) -> DataFrame:
    """
    Global year-over-year trend for each SDG indicator.
    Returns: year, item_code, item, global_avg, global_sum, country_count
    """
    return (
        df.groupBy("year", "item_code", "item")
          .agg(
              F.round(F.avg("value"), 3).alias("global_avg"),
              F.round(F.sum("value"), 1).alias("global_sum"),
              F.countDistinct("area_code").alias("country_count"),
              F.round(F.min("value"), 3).alias("min_value"),
              F.round(F.max("value"), 3).alias("max_value"),
              F.round(F.stddev("value"), 3).alias("std_value"),
          )
          .orderBy("item_code", "year")
    )


def regional_rankings(df: DataFrame, top_n: int = 20) -> DataFrame:
    """
    Rank regions by their average indicator value.
    Returns all (item_code, area) pairs ordered by avg descending.
    """
    agg = (
        df.groupBy("area_code", "area", "item_code", "item")
          .agg(
              F.round(F.avg("value"), 3).alias("avg_value"),
              F.round(F.max("value"), 3).alias("peak_value"),
              F.round(F.avg("growth_rate"), 3).alias("avg_growth_rate"),
              F.count("*").alias("observations"),
          )
    )
    window = Window.partitionBy("item_code").orderBy(F.col("avg_value").desc())
    return (
        agg.withColumn("rank", F.rank().over(window))
           .filter(F.col("rank") <= top_n)
           .orderBy("item_code", "rank")
    )


def growth_rate_analysis(df: DataFrame) -> DataFrame:
    """
    Compute absolute and relative growth between earliest and latest year
    for each (area, item_code) pair.
    """
    # Identify earliest and latest year per area/indicator
    win_asc  = Window.partitionBy("area_code", "item_code").orderBy("year")
    win_desc = Window.partitionBy("area_code", "item_code").orderBy(F.col("year").desc())

    first = (
        df.withColumn("rn", F.row_number().over(win_asc))
          .filter(F.col("rn") == 1)
          .select(
              F.col("area_code"),
              F.col("area"),
              F.col("item_code"),
              F.col("item"),
              F.col("year").alias("start_year"),
              F.col("value").alias("start_value"),
          )
    )
    last = (
        df.withColumn("rn", F.row_number().over(win_desc))
          .filter(F.col("rn") == 1)
          .select(
              F.col("area_code").alias("area_code_r"),
              F.col("item_code").alias("item_code_r"),
              F.col("year").alias("end_year"),
              F.col("value").alias("end_value"),
          )
    )
    # Join on both area_code AND item_code to avoid Cartesian product
    joined = first.join(
        last,
        (F.col("area_code") == F.col("area_code_r"))
        & (F.col("item_code") == F.col("item_code_r")),
        how="inner",
    ).drop("area_code_r", "item_code_r")
    return (
        joined
        .withColumn("absolute_change",
                    F.round(F.col("end_value") - F.col("start_value"), 3))
        .withColumn("pct_change",
                    F.round(
                        (F.col("end_value") - F.col("start_value"))
                        / F.col("start_value") * 100, 2
                    ))
        .orderBy("item_code", F.col("pct_change").desc())
    )


def affordability_metrics(diet_df: DataFrame,
                           staple_df: DataFrame,
                           unafford_df: DataFrame,
                           unable_df: DataFrame) -> DataFrame:
    """
    Join all four affordability datasets and compute derived metrics:
    - cost_gap   : healthy diet cost minus starchy staple cost
    - risk_score : (unaffordability_pct / 100) * people_millions
    """
    diet_df    = diet_df.withColumnRenamed("cost_usd", "healthy_diet_cost")
    staple_df  = staple_df.withColumnRenamed("cost_usd", "staple_cost")

    df = (
        diet_df
        .join(staple_df,  ["area", "year"], "left")
        .join(unafford_df, ["area", "year"], "left")
        .join(unable_df,   ["area", "year"], "left")
    )
    return (
        df
        .withColumn("cost_gap",
                    F.round(F.col("healthy_diet_cost") - F.col("staple_cost"), 4))
        .withColumn("risk_score",
                    F.round(
                        (F.col("prevalence_pct") / 100) * F.col("people_millions"), 3
                    ))
        .orderBy("year", F.col("risk_score").desc())
    )


def composite_risk_score(df: DataFrame) -> DataFrame:
    """
    Build a composite food security risk index per (area, year) using
    three normalised indicators:
      - Prevalence of undernourishment (24001)
      - Prevalence of food insecurity  (24003)
      - Growth rate (higher = worse)

    Score = mean of their Min-Max normalised values (range 0–1, higher = worse).
    """
    # Pivot to wide
    pivot = (
        df.filter(F.col("item_code").isin(24001, 24003))
          .groupBy("area_code", "area", "year")
          .pivot("item_code", [24001, 24003])
          .agg(F.first("value"))
          .withColumnRenamed("24001", "undernourishment_pct")
          .withColumnRenamed("24003", "insecurity_pct")
    )

    # Min-Max normalisation using window functions
    for col_name in ("undernourishment_pct", "insecurity_pct"):
        win = Window.partitionBy(F.lit(1))  # global window
        pivot = (
            pivot
            .withColumn(f"{col_name}_norm",
                        (F.col(col_name) - F.min(col_name).over(win))
                        / (F.max(col_name).over(win) - F.min(col_name).over(win)))
        )

    return (
        pivot
        .withColumn("composite_risk",
                    F.round(
                        (F.col("undernourishment_pct_norm")
                         + F.col("insecurity_pct_norm")) / 2, 4
                    ))
        .select("area_code", "area", "year",
                "undernourishment_pct", "insecurity_pct", "composite_risk")
        .orderBy("year", F.col("composite_risk").desc())
    )


def yoy_change(df: DataFrame,
               value_col: str = "global_avg",
               partition_col: str = "item_code") -> DataFrame:
    """
    Add year-over-year change columns to a yearly trend DataFrame.
    Input must have columns: year, <partition_col>, <value_col>
    """
    win = Window.partitionBy(partition_col).orderBy("year")
    return (
        df
        .withColumn("prev_value", F.lag(value_col).over(win))
        .withColumn("yoy_change",
                    F.round(F.col(value_col) - F.col("prev_value"), 4))
        .withColumn("yoy_pct_change",
                    F.round(
                        (F.col(value_col) - F.col("prev_value"))
                        / F.col("prev_value") * 100, 2
                    ))
    )


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def save(df: DataFrame, name: str, output_dir: str, fmt: str = "parquet") -> None:
    """Save a DataFrame as Parquet and CSV for downstream use."""
    path = os.path.join(output_dir, name)
    df.write.mode("overwrite").option("header", "true")
    if fmt == "parquet":
        df.coalesce(1).write.mode("overwrite").parquet(path + ".parquet")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path + ".csv")
    else:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"[save] {name}  →  {path}")


# ---------------------------------------------------------------------------
# Full aggregation pipeline
# ---------------------------------------------------------------------------

def run_aggregation_pipeline(hdfs_base: str = HDFS_OUTPUT_BASE,
                              output_dir: str = LOCAL_OUTPUT_DIR) -> None:
    """
    End-to-end Spark aggregation pipeline.

    Reads MapReduce outputs from HDFS (or local paths for testing),
    computes all aggregations, and saves results.
    """
    spark = get_spark()
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("LOADING MAPREDUCE OUTPUTS")
    print("=" * 60)

    hunger_df  = load_mapreduce_output(spark, hdfs_base, HUNGER_SCHEMA)
    diet_df    = load_mapreduce_output(spark, f"{hdfs_base}1", AFFORDABILITY_SCHEMA)
    unafford_df = (
        load_mapreduce_output(spark, f"{hdfs_base}2", AFFORDABILITY_SCHEMA)
        .withColumnRenamed("cost_usd", "prevalence_pct")
    )
    unable_df  = (
        load_mapreduce_output(spark, f"{hdfs_base}3", AFFORDABILITY_SCHEMA)
        .withColumnRenamed("cost_usd", "people_millions")
    )
    staple_df  = load_mapreduce_output(spark, f"{hdfs_base}4", AFFORDABILITY_SCHEMA)

    # Cache heavily reused DataFrames
    hunger_df.cache()
    diet_df.cache()

    print("\n" + "=" * 60)
    print("AGGREGATION 1 – Yearly global trend")
    print("=" * 60)
    trend_df = yearly_global_trend(hunger_df)
    trend_df = yoy_change(trend_df)
    trend_df.show(10, truncate=False)
    save(trend_df, "yearly_global_trend", output_dir)

    print("\n" + "=" * 60)
    print("AGGREGATION 2 – Regional rankings (top 20 per indicator)")
    print("=" * 60)
    rank_df = regional_rankings(hunger_df)
    rank_df.show(10, truncate=False)
    save(rank_df, "regional_rankings", output_dir)

    print("\n" + "=" * 60)
    print("AGGREGATION 3 – Growth rate analysis")
    print("=" * 60)
    growth_df = growth_rate_analysis(hunger_df)
    growth_df.show(10, truncate=False)
    save(growth_df, "growth_rate_analysis", output_dir)

    print("\n" + "=" * 60)
    print("AGGREGATION 4 – Affordability metrics")
    print("=" * 60)
    afford_df = affordability_metrics(diet_df, staple_df, unafford_df, unable_df)
    afford_df.show(10, truncate=False)
    save(afford_df, "affordability_metrics", output_dir)

    print("\n" + "=" * 60)
    print("AGGREGATION 5 – Composite risk scores")
    print("=" * 60)
    risk_df = composite_risk_score(hunger_df)
    risk_df.show(10, truncate=False)
    save(risk_df, "composite_risk_scores", output_dir)

    # Yearly affordability trend (diet cost global avg)
    print("\n" + "=" * 60)
    print("AGGREGATION 6 – Yearly affordability trend")
    print("=" * 60)
    diet_trend = (
        diet_df.groupBy("year")
               .agg(
                   F.round(F.avg("cost_usd"), 4).alias("avg_healthy_diet_cost"),
                   F.round(F.min("cost_usd"), 4).alias("min_cost"),
                   F.round(F.max("cost_usd"), 4).alias("max_cost"),
               )
               .orderBy("year")
    )
    # The diet cost trend is a single global time series (no extra partition dimension)
    # so we pass a constant literal as the partition column.
    diet_trend = yoy_change(diet_trend, value_col="avg_healthy_diet_cost",
                             partition_col="avg_healthy_diet_cost")
    # Re-derive without the self-partition hack: use a literal constant column
    from pyspark.sql.functions import lit
    diet_trend = (
        diet_df.groupBy("year")
               .agg(
                   F.round(F.avg("cost_usd"), 4).alias("avg_healthy_diet_cost"),
                   F.round(F.min("cost_usd"), 4).alias("min_cost"),
                   F.round(F.max("cost_usd"), 4).alias("max_cost"),
               )
               .orderBy("year")
               .withColumn("_part", lit(1))
    )
    win_global = Window.partitionBy("_part").orderBy("year")
    diet_trend = (
        diet_trend
        .withColumn("prev_value", F.lag("avg_healthy_diet_cost").over(win_global))
        .withColumn("yoy_change",
                    F.round(F.col("avg_healthy_diet_cost") - F.col("prev_value"), 4))
        .withColumn("yoy_pct_change",
                    F.round(
                        (F.col("avg_healthy_diet_cost") - F.col("prev_value"))
                        / F.col("prev_value") * 100, 2
                    ))
        .drop("_part")
    )
    diet_trend.show(10, truncate=False)
    save(diet_trend, "diet_cost_trend", output_dir)

    spark.stop()
    print("\nSpark aggregation pipeline complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Zero Hunger pipeline – Spark aggregation")
    parser.add_argument(
        "--hdfs-base", default=HDFS_OUTPUT_BASE,
        help="HDFS base path for MapReduce output directories (/output, /output1 …)")
    parser.add_argument(
        "--output-dir", default=LOCAL_OUTPUT_DIR,
        help="Local directory where Parquet / CSV results are saved")
    args = parser.parse_args()

    run_aggregation_pipeline(
        hdfs_base=args.hdfs_base,
        output_dir=args.output_dir,
    )
