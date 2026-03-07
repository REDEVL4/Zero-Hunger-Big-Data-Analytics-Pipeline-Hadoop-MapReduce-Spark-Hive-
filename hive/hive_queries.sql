-- =============================================================================
-- Zero Hunger Big Data Analytics Pipeline
-- Complete Hive Query Suite – All 7 SDG Food Security Indicators
--
-- Prerequisites:
--   1. HDFS output directories exist: /output, /output1, /output2, /output3, /output4
--   2. Hive metastore is initialised (schematool -initSchema -dbType derby)
--   3. Run: hive -f hive_queries.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- DATABASE
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS zero_hunger;
USE zero_hunger;

-- =============================================================================
-- EXTERNAL TABLES  (pointing at MapReduce output stored in HDFS)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Table 1: SDG Hunger Indicators (MapReduce Job 1 output)
--          Columns: year, area_code, area, item_code, item_code_sdg,
--                   item, value, unit, avg_value, growth_rate
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS sdg_hunger_indicators;
CREATE EXTERNAL TABLE sdg_hunger_indicators (
    year          INT,
    area_code     INT,
    area          STRING,
    item_code     INT,
    item_code_sdg STRING,
    item          STRING,
    value         DOUBLE,
    unit          STRING,
    avg_value     DOUBLE,
    growth_rate   DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/output';

-- ---------------------------------------------------------------------------
-- Table 2: Cost of Healthy Diet by Country/Year (Job 2 – /output1)
--          Columns: area, year, cost_usd
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS cost_healthy_diet;
CREATE EXTERNAL TABLE cost_healthy_diet (
    area     STRING,
    year     INT,
    cost_usd DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/output1';

-- ---------------------------------------------------------------------------
-- Table 3: Prevalence of Food Unaffordability % (Job 3 – /output2)
--          Columns: area, year, prevalence_pct
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS food_unaffordability;
CREATE EXTERNAL TABLE food_unaffordability (
    area           STRING,
    year           INT,
    prevalence_pct DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/output2';

-- ---------------------------------------------------------------------------
-- Table 4: Number Unable to Afford Healthy Diet (millions) (Job 4 – /output3)
--          Columns: area, year, people_millions
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS unable_afford_diet;
CREATE EXTERNAL TABLE unable_afford_diet (
    area           STRING,
    year           INT,
    people_millions DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/output3';

-- ---------------------------------------------------------------------------
-- Table 5: Cost of Starchy Staples (Job 5 – /output4)
--          Columns: area, year, cost_usd
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS cost_starchy_staples;
CREATE EXTERNAL TABLE cost_starchy_staples (
    area     STRING,
    year     INT,
    cost_usd DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/output4';


-- =============================================================================
-- MANAGED TABLES (for query results – stored in Hive warehouse)
-- =============================================================================

DROP TABLE IF EXISTS regional_hunger_summary;
CREATE TABLE regional_hunger_summary AS
SELECT
    area,
    year,
    item_code,
    item,
    ROUND(AVG(value), 2)         AS avg_value,
    ROUND(MAX(value), 2)         AS max_value,
    ROUND(MIN(value), 2)         AS min_value,
    ROUND(AVG(growth_rate), 2)   AS avg_growth_rate
FROM sdg_hunger_indicators
GROUP BY area, year, item_code, item
ORDER BY area, year, item_code;


-- =============================================================================
-- ANALYTICAL QUERIES  (SDG Indicator 1: Prevalence of Undernourishment – 24001)
-- =============================================================================

-- Q1-A: Top 20 regions by average prevalence of undernourishment
SELECT
    area,
    ROUND(AVG(value), 2) AS avg_undernourishment_pct,
    COUNT(DISTINCT year)  AS years_reported
FROM sdg_hunger_indicators
WHERE item_code = 24001
GROUP BY area
ORDER BY avg_undernourishment_pct DESC
LIMIT 20;

-- Q1-B: Global year-over-year trend for undernourishment
SELECT
    year,
    ROUND(AVG(value), 2)      AS global_avg_pct,
    ROUND(SUM(value), 2)      AS global_total,
    COUNT(DISTINCT area_code) AS countries_reporting
FROM sdg_hunger_indicators
WHERE item_code = 24001
GROUP BY year
ORDER BY year;

-- Q1-C: Countries with worsening undernourishment (positive growth rate)
SELECT
    area,
    ROUND(MAX(growth_rate), 2) AS total_growth_pct
FROM sdg_hunger_indicators
WHERE item_code = 24001
  AND growth_rate > 0
GROUP BY area
ORDER BY total_growth_pct DESC
LIMIT 20;


-- =============================================================================
-- SDG Indicator 2: Prevalence of Moderate or Severe Food Insecurity (24003)
-- =============================================================================

-- Q2-A: Top 15 countries by 2022 moderate/severe food insecurity
SELECT
    area,
    value AS food_insecurity_pct_2022
FROM sdg_hunger_indicators
WHERE item_code = 24003
  AND year = 2022
ORDER BY value DESC
LIMIT 15;

-- Q2-B: Regional comparison – latest available year
SELECT
    area,
    MAX(year)           AS latest_year,
    ROUND(AVG(value), 2) AS avg_food_insecurity_pct
FROM sdg_hunger_indicators
WHERE item_code = 24003
GROUP BY area
ORDER BY avg_food_insecurity_pct DESC;

-- Q2-C: Year-over-year change in food insecurity (2000–2023)
SELECT
    year,
    ROUND(AVG(value), 2)  AS avg_global_insecurity_pct,
    COUNT(DISTINCT area)  AS regions_reporting
FROM sdg_hunger_indicators
WHERE item_code = 24003
GROUP BY year
ORDER BY year;


-- =============================================================================
-- SDG Indicator 3: Number Severely Food Insecure (24004)
-- =============================================================================

-- Q3-A: Absolute numbers – top 10 regions, latest year
SELECT
    area,
    year,
    ROUND(value, 1)  AS severely_food_insecure_thousands,
    unit
FROM sdg_hunger_indicators
WHERE item_code = 24004
  AND year = (SELECT MAX(year) FROM sdg_hunger_indicators WHERE item_code = 24004)
ORDER BY value DESC
LIMIT 10;

-- Q3-B: Trend – severely food insecure population globally (2000–2023)
SELECT
    year,
    ROUND(SUM(value), 0)  AS total_severely_insecure_thousands,
    COUNT(DISTINCT area)  AS regions
FROM sdg_hunger_indicators
WHERE item_code = 24004
GROUP BY year
ORDER BY year;

-- Q3-C: Countries with highest absolute growth (start vs end year)
SELECT
    t1.area,
    t1.value                                    AS value_start,
    t2.value                                    AS value_end,
    ROUND(t2.value - t1.value, 1)               AS absolute_increase,
    ROUND((t2.value - t1.value) / t1.value * 100, 2) AS pct_change
FROM sdg_hunger_indicators t1
JOIN sdg_hunger_indicators t2
  ON t1.area = t2.area
 AND t1.item_code = t2.item_code
WHERE t1.item_code = 24004
  AND t1.year = 2000
  AND t2.year = 2022
ORDER BY pct_change DESC
LIMIT 15;


-- =============================================================================
-- SDG Indicator 4: Number Moderately or Severely Food Insecure (24005)
-- =============================================================================

-- Q4-A: Top regions for total (moderate + severe) food insecurity
SELECT
    area,
    ROUND(AVG(value), 1) AS avg_mod_severe_thousands,
    unit
FROM sdg_hunger_indicators
WHERE item_code = 24005
GROUP BY area, unit
ORDER BY avg_mod_severe_thousands DESC
LIMIT 20;

-- Q4-B: Africa vs Asia comparison (aggregate)
SELECT
    CASE
        WHEN area LIKE '%Africa%'  THEN 'Africa'
        WHEN area LIKE '%Asia%'    THEN 'Asia'
        WHEN area LIKE '%America%' THEN 'Americas'
        WHEN area LIKE '%Europe%'  THEN 'Europe'
        ELSE 'Other'
    END AS region_group,
    year,
    ROUND(SUM(value), 0) AS total_insecure_thousands
FROM sdg_hunger_indicators
WHERE item_code = 24005
GROUP BY
    CASE
        WHEN area LIKE '%Africa%'  THEN 'Africa'
        WHEN area LIKE '%Asia%'    THEN 'Asia'
        WHEN area LIKE '%America%' THEN 'Americas'
        WHEN area LIKE '%Europe%'  THEN 'Europe'
        ELSE 'Other'
    END,
    year
ORDER BY region_group, year;


-- =============================================================================
-- SDG Indicator 5: Cost of Healthy Diet (Item Code 7004 → /output1)
-- =============================================================================

-- Q5-A: Countries where healthy diet costs most (latest year)
SELECT
    area,
    year,
    ROUND(cost_usd, 4) AS cost_per_day_usd
FROM cost_healthy_diet
WHERE year = (SELECT MAX(year) FROM cost_healthy_diet)
ORDER BY cost_usd DESC
LIMIT 20;

-- Q5-B: Global average cost trend
SELECT
    year,
    ROUND(AVG(cost_usd), 4)  AS global_avg_cost_usd,
    ROUND(MIN(cost_usd), 4)  AS min_cost,
    ROUND(MAX(cost_usd), 4)  AS max_cost
FROM cost_healthy_diet
GROUP BY year
ORDER BY year;

-- Q5-C: Countries where cost exceeded $4 per day (>global average ~2022)
SELECT
    area,
    year,
    ROUND(cost_usd, 4) AS cost_per_day_usd
FROM cost_healthy_diet
WHERE cost_usd > 4.0
ORDER BY cost_usd DESC;


-- =============================================================================
-- SDG Indicator 6: Prevalence of Food Unaffordability (7005 → /output2)
-- =============================================================================

-- Q6-A: Highest unaffordability rates (most recent year)
SELECT
    area,
    year,
    ROUND(prevalence_pct, 2) AS unaffordability_pct
FROM food_unaffordability
WHERE year = (SELECT MAX(year) FROM food_unaffordability)
ORDER BY prevalence_pct DESC
LIMIT 20;

-- Q6-B: Trend over time (global average)
SELECT
    year,
    ROUND(AVG(prevalence_pct), 2) AS global_avg_pct,
    COUNT(DISTINCT area)          AS countries
FROM food_unaffordability
GROUP BY year
ORDER BY year;

-- Q6-C: Countries that improved unaffordability by >= 5pp over 5 years
SELECT
    t1.area,
    t1.prevalence_pct                                   AS pct_2017,
    t2.prevalence_pct                                   AS pct_2022,
    ROUND(t1.prevalence_pct - t2.prevalence_pct, 2)    AS improvement_pp
FROM food_unaffordability t1
JOIN food_unaffordability t2
  ON t1.area = t2.area
WHERE t1.year = 2017
  AND t2.year = 2022
  AND (t1.prevalence_pct - t2.prevalence_pct) >= 5
ORDER BY improvement_pp DESC;


-- =============================================================================
-- SDG Indicator 7: Cost of Starchy Staples (7007 → /output4)
-- =============================================================================

-- Q7-A: Starchy staple cost by country (latest year)
SELECT
    area,
    year,
    ROUND(cost_usd, 4) AS staple_cost_per_day_usd
FROM cost_starchy_staples
WHERE year = (SELECT MAX(year) FROM cost_starchy_staples)
ORDER BY cost_usd DESC
LIMIT 20;

-- Q7-B: Cost gap between healthy diet and starchy staples (proxy for diet quality)
SELECT
    c.area,
    c.year,
    ROUND(c.cost_usd, 4)           AS healthy_diet_cost,
    ROUND(s.cost_usd, 4)           AS staple_cost,
    ROUND(c.cost_usd - s.cost_usd, 4) AS cost_gap_usd
FROM cost_healthy_diet c
JOIN cost_starchy_staples s
  ON c.area = s.area
 AND c.year = s.year
WHERE c.year = 2022
ORDER BY cost_gap_usd DESC
LIMIT 20;

-- Q7-C: Year-over-year staple cost inflation
SELECT
    year,
    ROUND(AVG(cost_usd), 4) AS avg_staple_cost,
    ROUND(MAX(cost_usd), 4) AS max_staple_cost
FROM cost_starchy_staples
GROUP BY year
ORDER BY year;


-- =============================================================================
-- CROSS-INDICATOR ANALYSIS
-- =============================================================================

-- CX-1: Hotspot index – countries simultaneously high on 3+ indicators
--        (undernourishment + food insecurity + unaffordability)
SELECT
    u.area,
    u.year,
    ROUND(u.value, 2)               AS undernourishment_pct,
    ROUND(fi.value, 2)              AS food_insecurity_pct,
    ROUND(ua.prevalence_pct, 2)     AS unaffordability_pct,
    ROUND(
        (u.value / 100) + (fi.value / 100) + (ua.prevalence_pct / 100), 4
    )                               AS composite_risk_score
FROM sdg_hunger_indicators u
JOIN sdg_hunger_indicators fi
  ON u.area = fi.area AND u.year = fi.year AND fi.item_code = 24003
JOIN food_unaffordability ua
  ON u.area = ua.area AND u.year = ua.year
WHERE u.item_code  = 24001
  AND u.year = 2022
ORDER BY composite_risk_score DESC
LIMIT 20;

-- CX-2: Correlation proxy – average diet cost vs average food insecurity by year
SELECT
    d.year,
    ROUND(AVG(d.cost_usd), 4)      AS avg_healthy_diet_cost,
    ROUND(AVG(u.prevalence_pct), 2) AS avg_unaffordability_pct
FROM cost_healthy_diet d
JOIN food_unaffordability u
  ON d.area = u.area AND d.year = u.year
GROUP BY d.year
ORDER BY d.year;

-- CX-3: Yearly progress report – count of countries meeting SDG2 thresholds
--        (<5% undernourishment is WHO "low" threshold)
SELECT
    year,
    COUNT(DISTINCT CASE WHEN value < 5  THEN area END)  AS countries_low_hunger,
    COUNT(DISTINCT CASE WHEN value < 15 THEN area END)  AS countries_moderate_hunger,
    COUNT(DISTINCT CASE WHEN value >= 15 THEN area END) AS countries_high_hunger
FROM sdg_hunger_indicators
WHERE item_code = 24001
GROUP BY year
ORDER BY year;


-- =============================================================================
-- EXPORT VIEWS  (for use by Spark / visualization layer)
-- =============================================================================

DROP VIEW IF EXISTS vw_hunger_hotspots;
CREATE VIEW vw_hunger_hotspots AS
SELECT
    area,
    year,
    item_code,
    item,
    ROUND(value, 2)       AS value,
    unit,
    ROUND(avg_value, 2)   AS avg_value,
    ROUND(growth_rate, 2) AS growth_rate
FROM sdg_hunger_indicators
WHERE item_code IN (24001, 24003, 24004, 24005)
  AND year >= 2015;

DROP VIEW IF EXISTS vw_affordability_summary;
CREATE VIEW vw_affordability_summary AS
SELECT
    d.area,
    d.year,
    ROUND(d.cost_usd, 4)        AS healthy_diet_cost,
    ROUND(s.cost_usd, 4)        AS staple_cost,
    ROUND(ua.prevalence_pct, 2) AS unaffordability_pct,
    ROUND(n.people_millions, 2) AS people_unable_to_afford_millions
FROM cost_healthy_diet d
LEFT JOIN cost_starchy_staples  s  ON d.area = s.area  AND d.year = s.year
LEFT JOIN food_unaffordability  ua ON d.area = ua.area AND d.year = ua.year
LEFT JOIN unable_afford_diet    n  ON d.area = n.area  AND d.year = n.year;
