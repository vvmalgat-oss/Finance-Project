-- ===================================================================
-- QUALITY ANALYSIS ON U.S. STOCKS - SQL QUERIES
-- ===================================================================
-- S&P 500 Financial Quality Scoring Framework
-- Database: Google BigQuery
-- Project: projectsPractice | Dataset: sp500dataAnalysis | Table: sp500data
-- Purpose: Build defensible quality scoring model for 503 S&P 500 companies
-- ===================================================================

-- ===================================================================
-- SECTION 1: SENSE CHECKS & DATA VALIDATION
-- ===================================================================

-- 1.1: SAMPLE ROWS
-- Purpose: Verify dataset structure and review first 5 records
SELECT *
FROM `projectspractice.sp500dataanalysis.sp500data` 
LIMIT 5;

-- ===================================================================

-- 1.2: ROW & TICKER COUNT VALIDATION
-- Purpose: Detect duplicates (row_count should equal ticker_count)
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT symbol) AS ticker_count
FROM `projectspractice.sp500dataanalysis.sp500data`;

-- Expected: 503 rows, 503 unique symbols (no duplicates)

-- ===================================================================

-- 1.3: NULL VALUE PROFILING
-- Purpose: Identify missing data patterns across all fields
SELECT
  SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS null_symbol,
  SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_name,
  SUM(CASE WHEN sector IS NULL THEN 1 ELSE 0 END) AS null_sector,
  SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
  SUM(CASE WHEN priceToEarnings IS NULL THEN 1 ELSE 0 END) AS null_pe,
  SUM(CASE WHEN dividendYield IS NULL THEN 1 ELSE 0 END) AS null_dividend,
  SUM(CASE WHEN EarningPerShare IS NULL THEN 1 ELSE 0 END) AS null_eps,
  SUM(CASE WHEN WeekLow52 IS NULL THEN 1 ELSE 0 END) AS null_wk_low,
  SUM(CASE WHEN WeekHigh52 IS NULL THEN 1 ELSE 0 END) AS null_wk_high,
  SUM(CASE WHEN marketCap IS NULL THEN 1 ELSE 0 END) AS null_mktcap,
  SUM(CASE WHEN ebitda IS NULL THEN 1 ELSE 0 END) AS null_ebitda,
  SUM(CASE WHEN PriceToSales IS NULL THEN 1 ELSE 0 END) AS null_ps,
  SUM(CASE WHEN priceToBook IS NULL THEN 1 ELSE 0 END) AS null_pb
FROM `projectspractice.sp500dataanalysis.sp500data`;

-- Note: null_dividend ~100-150 expected (non-dividend payers)
-- Note: null_ebitda ~20-50 expected (not all companies report)

-- ===================================================================

-- 1.4: RANGE CHECK - IDENTIFY IMPOSSIBLE VALUES
-- Purpose: Spot values that violate logical bounds
SELECT
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  MIN(priceToEarnings) AS min_pe,
  MAX(priceToEarnings) AS max_pe,
  MIN(dividendYield) AS min_dividend,
  MAX(dividendYield) AS max_dividend,
  MIN(EarningPerShare) AS min_eps,
  MAX(EarningPerShare) AS max_eps,
  MIN(WeekLow52) AS min_wk_low,
  MAX(WeekHigh52) AS max_wk_high,
  MIN(marketCap) AS min_mktcap,
  MAX(marketCap) AS max_mktcap,
  MIN(ebitda) AS min_ebitda,
  MAX(ebitda) AS max_ebitda,
  MIN(PriceToSales) AS min_ps,
  MAX(PriceToSales) AS max_ps,
  MIN(priceToBook) AS min_pb,
  MAX(priceToBook) AS max_pb
FROM `projectspractice.sp500dataanalysis.sp500data`;

-- ===================================================================

-- 1.5: CROSS-FIELD VALIDATION - PRICE VS 52-WEEK RANGE
-- Purpose: Verify price falls between 52-week low and high
SELECT
  COUNT(*) AS rows_checked,
  COUNTIF(WeekLow52 IS NOT NULL AND WeekHigh52 IS NOT NULL
    AND (price < WeekLow52 OR price > WeekHigh52)) AS price_outside_range,
  COUNTIF(WeekLow52 IS NOT NULL AND WeekHigh52 IS NOT NULL
    AND WeekLow52 > WeekHigh52) AS inverted_range
FROM `projectspractice.sp500dataanalysis.sp500data`;

-- ===================================================================

-- 1.6: CROSS-FIELD VALIDATION - CALCULATED P/E VS REPORTED P/E
-- Purpose: Verify P/E = price / EPS (tolerance: ±2)
SELECT
  COUNT(*) AS rows_checked,
  COUNTIF(ABS((price / NULLIF(EarningPerShare, 0)) - priceToEarnings) > 2) 
    AS pe_mismatch_gt2
FROM `projectspractice.sp500dataanalysis.sp500data`;

-- ===================================================================

-- 1.7: DISTRIBUTION ANALYSIS BY SECTOR
-- Purpose: Identify sector composition and bad price data
SELECT
  sector,
  COUNT(*) AS n,
  COUNTIF(price IS NULL OR price <= 0) AS bad_price_rows
FROM `projectspractice.sp500dataanalysis.sp500data`
GROUP BY sector
ORDER BY n DESC;

-- ===================================================================

-- 1.8: DISTRIBUTION PROFILING USING DECILES
-- Purpose: Understand data shape before normalisation
SELECT
  APPROX_QUANTILES(priceToEarnings, 10) AS pe_deciles,
  APPROX_QUANTILES(ebitda, 10) AS ebitda_deciles,
  APPROX_QUANTILES(dividendYield, 10) AS divy_deciles,
  APPROX_QUANTILES(PriceToSales, 10) AS ps_deciles,
  APPROX_QUANTILES(priceToBook, 10) AS pb_deciles,
  APPROX_QUANTILES(marketCap, 10) AS mktcap_deciles
FROM `projectspractice.sp500dataanalysis.sp500data`;

-- ===================================================================

-- 1.9: SECTOR-LEVEL NULL DISTRIBUTION
-- Purpose: Detect if missing data concentrates in specific sectors
SELECT
  sector,
  COUNT(*) AS n,
  COUNTIF(priceToEarnings IS NULL) AS null_pe,
  COUNTIF(dividendYield IS NULL) AS null_dividend,
  COUNTIF(ebitda IS NULL) AS null_ebitda
FROM `projectspractice.sp500dataanalysis.sp500data`
GROUP BY sector
ORDER BY n DESC;

-- ===================================================================
-- SECTION 2: CREATE CLEAN VIEW
-- ===================================================================

-- Purpose: Filter out invalid records (price ≤ 0, marketCap ≤ 0, etc.)
-- Creates analysis-ready dataset

CREATE OR REPLACE VIEW `projectspractice.sp500dataanalysis.sp500data_clean` AS
SELECT
  symbol,
  name,
  sector,
  price,
  priceToEarnings,
  dividendYield,
  EarningPerShare,
  WeekLow52,
  WeekHigh52,
  marketCap,
  ebitda,
  PriceToSales,
  priceToBook
FROM `projectspractice.sp500dataanalysis.sp500data`
WHERE price > 0
  AND marketCap > 0
  AND PriceToSales > 0
  AND priceToBook > 0
  AND priceToEarnings > 0;

-- ===================================================================
-- SECTION 3: DATA CLEANING & STANDARDISATION
-- ===================================================================

-- 3.1: HANDLE MISSING VALUES
-- Replace NULL dividendYield and ebitda with 0
-- Rationale: Absence = business reality (no dividend policy, not reported)

CREATE OR REPLACE VIEW `projectspractice.sp500dataanalysis.sp500data_clean_step1` AS
SELECT
  symbol,
  name,
  INITCAP(TRIM(sector)) AS sector,
  price,
  priceToEarnings,
  IFNULL(dividendYield, 0) AS dividendYield,
  EarningPerShare,
  WeekLow52,
  WeekHigh52,
  marketCap,
  IFNULL(ebitda, 0) AS ebitda,
  PriceToSales,
  priceToBook
FROM `projectspractice.sp500dataanalysis.sp500data_clean`;

-- ===================================================================

-- 3.2: STANDARDISE SECTOR LABELS
-- Map inconsistent sector names into 11 standardised categories

CREATE OR REPLACE VIEW `projectspractice.sp500dataanalysis.sp500data_clean_step2` AS
WITH mapped AS (
  SELECT
    symbol,
    name,
    CASE
      WHEN sector IS NULL OR TRIM(sector) = '' THEN 'Unclassified'
      WHEN UPPER(sector) LIKE '%REIT%' OR UPPER(sector) LIKE '%REAL ESTATE%'
        THEN 'Real Estate'
      WHEN UPPER(sector) LIKE '%BANK%' OR UPPER(sector) LIKE '%INSUR%'
        OR UPPER(sector) LIKE '%CAPITAL MARKET%' OR UPPER(sector) LIKE '%FINAN%'
        OR UPPER(sector) LIKE '%ASSET MANAGE%' OR UPPER(sector) LIKE '%BROKER%'
        THEN 'Financials'
      WHEN UPPER(sector) LIKE '%SOFTWARE%' OR UPPER(sector) LIKE '%SEMICON%'
        OR UPPER(sector) LIKE '%TECH%' OR UPPER(sector) LIKE '%HARDWARE%'
        OR UPPER(sector) LIKE '%IT %' OR UPPER(sector) = 'IT'
        OR UPPER(sector) LIKE '%DATA%' OR UPPER(sector) LIKE '%ELECTRONIC%'
        THEN 'Information Technology'
      WHEN UPPER(sector) LIKE '%TELECOM%' OR UPPER(sector) LIKE '%COMMUNICAT%'
        OR UPPER(sector) LIKE '%MEDIA%' OR UPPER(sector) LIKE '%ENTERTAIN%'
        OR UPPER(sector) LIKE '%INTERACTIVE%'
        THEN 'Communication Services'
      WHEN UPPER(sector) LIKE '%HEALTH%' OR UPPER(sector) LIKE '%PHARM%'
        OR UPPER(sector) LIKE '%BIOTECH%' OR UPPER(sector) LIKE '%MEDICAL%'
        OR UPPER(sector) LIKE '%LIFE SCIENCE%'
        THEN 'Health Care'
      WHEN UPPER(sector) LIKE '%CONSUMER STAPLES%' OR UPPER(sector) LIKE '%CONSUMER DEFENSIVE%'
        OR UPPER(sector) LIKE '%FOOD%' OR UPPER(sector) LIKE '%BEVERAGE%'
        OR UPPER(sector) LIKE '%TOBACCO%' OR UPPER(sector) LIKE '%HOUSEHOLD%'
        OR UPPER(sector) LIKE '%PERSONAL PRODUCT%' OR UPPER(sector) LIKE '%STAPLES RETAIL%'
        THEN 'Consumer Staples'
      WHEN UPPER(sector) LIKE '%CONSUMER DISCRETIONARY%' OR UPPER(sector) LIKE '%AUTOM%'
        OR UPPER(sector) LIKE '%RETAIL%' OR UPPER(sector) LIKE '%TEXTILE%'
        OR UPPER(sector) LIKE '%APPAREL%' OR UPPER(sector) LIKE '%LUXURY%'
        OR UPPER(sector) LIKE '%HOTEL%' OR UPPER(sector) LIKE '%RESTAURANT%'
        OR UPPER(sector) LIKE '%LEISURE%' OR UPPER(sector) LIKE '%E-COMMERCE%'
        THEN 'Consumer'
      WHEN UPPER(sector) LIKE '%INDUSTR%' OR UPPER(sector) LIKE '%AEROSPACE%'
        OR UPPER(sector) LIKE '%DEFENSE%' OR UPPER(sector) LIKE '%MACHIN%'
        OR UPPER(sector) LIKE '%CONSTRUCTION%' OR UPPER(sector) LIKE '%ENGINEER%'
        OR UPPER(sector) LIKE '%TRANSPORT%' OR UPPER(sector) LIKE '%LOGISTIC%'
        OR UPPER(sector) LIKE '%COMMERCIAL SERVICES%'
        THEN 'Industrials'
      WHEN UPPER(sector) LIKE '%MATERIAL%' OR UPPER(sector) LIKE '%CHEMIC%'
        OR UPPER(sector) LIKE '%METAL%' OR UPPER(sector) LIKE '%MINING%'
        OR UPPER(sector) LIKE '%PAPER%' OR UPPER(sector) LIKE '%FOREST%'
        THEN 'Materials'
      WHEN UPPER(sector) LIKE '%ENERGY%' OR UPPER(sector) LIKE '%OIL%'
        OR UPPER(sector) LIKE '%GAS%' OR UPPER(sector) LIKE '%COAL%'
        OR UPPER(sector) LIKE '%RENEWABLE%' OR UPPER(sector) LIKE '%SOLAR%'
        THEN 'Energy'
      WHEN UPPER(sector) LIKE '%UTILITY%' OR UPPER(sector) LIKE '%ELECTRIC%'
        OR UPPER(sector) LIKE '%WATER%' OR UPPER(sector) LIKE '%POWER%'
        OR UPPER(sector) LIKE '%GAS UTIL%'
        THEN 'Utilities'
      ELSE sector
    END AS sector_std,
    price,
    priceToEarnings,
    dividendYield,
    EarningPerShare,
    WeekLow52,
    WeekHigh52,
    marketCap,
    ebitda,
    PriceToSales,
    priceToBook
  FROM `projectspractice.sp500dataanalysis.sp500data_clean_step1`
)
SELECT
  symbol,
  name,
  sector_std AS sector,
  price,
  priceToEarnings,
  dividendYield,
  EarningPerShare,
  WeekLow52,
  WeekHigh52,
  marketCap,
  ebitda,
  PriceToSales,
  priceToBook
FROM mapped
WHERE PriceToSales >= 0 AND PriceToSales <= 12.78
  AND priceToEarnings >= 0 AND priceToEarnings <= 69.185;

-- Note: P/S and P/E caps based on IQR thresholds to reduce outlier skew

-- ===================================================================
-- SECTION 4: DATA VALIDATION CHECKS
-- ===================================================================

-- 4.1: VERIFY CLEANED DATASET
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT symbol) AS unique_symbols,
  COUNT(DISTINCT sector) AS sectors
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`;

-- ===================================================================

-- 4.2: DISTRIBUTION BY SECTOR
SELECT
  sector,
  COUNT(*) AS n,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
GROUP BY sector
ORDER BY n DESC;

-- ===================================================================

-- 4.3: SUMMARY STATISTICS
SELECT
  'price' AS metric,
  MIN(price) AS min,
  MAX(price) AS max,
  AVG(price) AS avg
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'priceToEarnings', MIN(priceToEarnings), MAX(priceToEarnings), AVG(priceToEarnings)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'dividendYield', MIN(dividendYield), MAX(dividendYield), AVG(dividendYield)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'ebitda', MIN(ebitda), MAX(ebitda), AVG(ebitda)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'PriceToSales', MIN(PriceToSales), MAX(PriceToSales), AVG(PriceToSales)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'priceToBook', MIN(priceToBook), MAX(priceToBook), AVG(priceToBook)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`;

-- ===================================================================

-- 4.4: NULL VALUE CHECK AFTER CLEANING
SELECT
  'price' AS metric,
  COUNTIF(price IS NULL) AS missing
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'priceToEarnings', COUNTIF(priceToEarnings IS NULL)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'dividendYield', COUNTIF(dividendYield IS NULL)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'ebitda', COUNTIF(ebitda IS NULL)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'PriceToSales', COUNTIF(PriceToSales IS NULL)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
UNION ALL
SELECT 'priceToBook', COUNTIF(priceToBook IS NULL)
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`;

-- ===================================================================

-- 4.5: OUTLIER DETECTION USING IQR
WITH bounds AS (
  SELECT
    metric,
    Q1,
    Q3,
    (Q3 - Q1) AS iqr,
    Q1 - 1.5 * (Q3 - Q1) AS lower_bound,
    Q3 + 1.5 * (Q3 - Q1) AS upper_bound
  FROM (
    SELECT
      'priceToEarnings' AS metric,
      APPROX_QUANTILES(priceToEarnings, 4)[OFFSET(1)] AS Q1,
      APPROX_QUANTILES(priceToEarnings, 4)[OFFSET(3)] AS Q3
    FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
    UNION ALL
    SELECT
      'PriceToSales',
      APPROX_QUANTILES(PriceToSales, 4)[OFFSET(1)],
      APPROX_QUANTILES(PriceToSales, 4)[OFFSET(3)]
    FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`
  )
)
SELECT
  metric,
  lower_bound,
  upper_bound
FROM bounds;

-- ===================================================================
-- SECTION 5: NORMALISATION TO 0-100 SCALE
-- ===================================================================

-- Purpose: Convert 5 metrics to common 0-100 scale
-- Valuation metrics (P/E, P/S, P/B): Lower = Better → Reverse formula
-- Profitability metrics (EBITDA, Dividend Yield): Higher = Better → Direct formula

CREATE OR REPLACE VIEW `projectspractice.sp500dataanalysis.sp500data_norm` AS
SELECT
  symbol,
  name,
  sector,
  price,
  priceToEarnings,
  dividendYield,
  EarningPerShare,
  WeekLow52,
  WeekHigh52,
  marketCap,
  ebitda,
  PriceToSales,
  priceToBook,
  -- VALUATION METRICS: Reverse (lower = higher score)
  ROUND(
    100 * (MAX(priceToEarnings) OVER() - priceToEarnings)
    / NULLIF(MAX(priceToEarnings) OVER() - MIN(priceToEarnings) OVER(), 0),
    2
  ) AS pe_norm,
  ROUND(
    100 * (MAX(PriceToSales) OVER() - PriceToSales)
    / NULLIF(MAX(PriceToSales) OVER() - MIN(PriceToSales) OVER(), 0),
    2
  ) AS ps_norm,
  ROUND(
    100 * (MAX(priceToBook) OVER() - priceToBook)
    / NULLIF(MAX(priceToBook) OVER() - MIN(priceToBook) OVER(), 0),
    2
  ) AS pb_norm,
  -- PROFITABILITY METRICS: Direct (higher = higher score)
  ROUND(
    100 * (ebitda - MIN(ebitda) OVER())
    / NULLIF(MAX(ebitda) OVER() - MIN(ebitda) OVER(), 0),
    2
  ) AS ebitda_norm,
  ROUND(
    100 * (dividendYield - MIN(dividendYield) OVER())
    / NULLIF(MAX(dividendYield) OVER() - MIN(dividendYield) OVER(), 0),
    2
  ) AS divy_norm
FROM `projectspractice.sp500dataanalysis.sp500data_clean_step2`;

-- ===================================================================
-- SECTION 6: QUALITY SCORE CALCULATION & RANKING
-- ===================================================================

CREATE OR REPLACE VIEW `projectspractice.sp500dataanalysis.sp500data_qualityScore` AS
SELECT
  symbol,
  name,
  sector,
  price,
  marketCap,
  priceToEarnings,
  pe_norm,
  ebitda,
  ebitda_norm,
  dividendYield,
  divy_norm,
  PriceToSales,
  ps_norm,
  priceToBook,
  pb_norm,
  -- QUALITY SCORE: Equal-weighted average of 5 components
  ROUND((pe_norm + ebitda_norm + divy_norm + ps_norm + pb_norm) / 5, 2) AS quality_score,
  -- OVERALL RANK: Position vs. all 500 companies
  ROW_NUMBER() OVER (ORDER BY (pe_norm + ebitda_norm + divy_norm + ps_norm + pb_norm) DESC) AS rank_overall,
  -- SECTOR RANK: Position vs. sector peers
  DENSE_RANK() OVER (
    PARTITION BY sector
    ORDER BY (pe_norm + ebitda_norm + divy_norm + ps_norm + pb_norm) DESC
  ) AS rank_in_sector
FROM `projectspractice.sp500dataanalysis.sp500data_norm`;

-- ===================================================================
-- SECTION 7: ANALYSIS QUERIES
-- ===================================================================

-- 7.1: VIEW COMPLETE RANKINGS (ORDERED BY QUALITY SCORE)
SELECT
  symbol,
  name,
  sector,
  quality_score,
  rank_overall,
  rank_in_sector,
  pe_norm,
  ps_norm,
  pb_norm,
  ebitda_norm,
  divy_norm
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`
ORDER BY rank_overall ASC;

-- ===================================================================

-- 7.2: TOP 10 PERFORMERS
SELECT
  symbol,
  name,
  sector,
  quality_score,
  rank_overall,
  pe_norm,
  ps_norm,
  pb_norm,
  ebitda_norm,
  divy_norm
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`
ORDER BY quality_score DESC
LIMIT 10;

-- ===================================================================

-- 7.3: BOTTOM 10 PERFORMERS
SELECT
  symbol,
  name,
  sector,
  quality_score,
  rank_overall,
  pe_norm,
  ps_norm,
  pb_norm,
  ebitda_norm,
  divy_norm
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`
ORDER BY quality_score ASC
LIMIT 10;

-- ===================================================================

-- 7.4: SECTOR SUMMARY
CREATE OR REPLACE TABLE `projectspractice.sp500dataanalysis.sp500data_sector_summary` AS
SELECT
  sector,
  ROUND(AVG(quality_score), 2) AS avg_quality_score,
  MIN(quality_score) AS min_quality_score,
  MAX(quality_score) AS max_quality_score,
  COUNT(*) AS n_symbols
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`
GROUP BY sector
ORDER BY avg_quality_score DESC;

-- ===================================================================

-- 7.5: SECTOR LEADERS (Rank #1 in each sector)
SELECT
  sector,
  symbol,
  name,
  quality_score,
  rank_in_sector,
  pe_norm,
  ps_norm,
  pb_norm,
  ebitda_norm,
  divy_norm
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`
WHERE rank_in_sector = 1
ORDER BY quality_score DESC;

-- ===================================================================

-- 7.6: SECTOR AGGREGATES
SELECT
  sector,
  ROUND(AVG(quality_score), 2) AS avg_quality_score,
  MIN(quality_score) AS min_quality_score,
  MAX(quality_score) AS max_quality_score,
  COUNT(*) AS n_symbols
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`
GROUP BY sector
ORDER BY avg_quality_score DESC;

-- ===================================================================

-- 7.7: DRIVER ANALYSIS - CORRELATION OF COMPONENTS
-- Purpose: Understand which metrics drive most score variation
SELECT
  CORR(quality_score, pe_norm) AS corr_pe,
  CORR(quality_score, ps_norm) AS corr_ps,
  CORR(quality_score, pb_norm) AS corr_pb,
  CORR(quality_score, ebitda_norm) AS corr_ebitda,
  CORR(quality_score, divy_norm) AS corr_divy
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`;

-- ===================================================================
-- SECTION 8: EXPORT QUERY FOR TABLEAU
-- ===================================================================

-- Purpose: Final dataset to export as CSV for Tableau dashboard

SELECT
  symbol,
  name,
  sector,
  price,
  priceToEarnings,
  dividendYield,
  ebitda,
  marketCap,
  PriceToSales,
  priceToBook,
  pe_norm,
  ps_norm,
  pb_norm,
  ebitda_norm,
  divy_norm,
  quality_score,
  rank_overall,
  rank_in_sector
FROM `projectspractice.sp500dataanalysis.sp500data_qualityScore`
ORDER BY rank_overall ASC;

-- Export steps:
-- 1. Run query in BigQuery console
-- 2. Click "SAVE RESULTS" or "Export" button
-- 3. Download as CSV
-- 4. Import into Tableau

-- ===================================================================
-- END OF SQL QUERIES
-- ===================================================================
