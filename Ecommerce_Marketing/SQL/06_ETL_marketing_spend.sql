-- ============================================================
-- FILE:    06_ETL_marketing_spend.sql
-- ORDER:   Run after 01_Schema.sql
-- PROJECT: E-commerce Marketing ROI & Customer Acquisition
--
-- PURPOSE:
-- Cleans and transforms raw marketing spend data from
-- stg_marketing_spend and loads it into the production
-- marketing_spend table.
--
-- PREREQUISITE: 01_Schema.sql must be run first
--
-- RAW SOURCE:  stg_marketing_spend  (3,650 rows)
-- TARGET:      marketing_spend      (3,424 rows after cleaning)
--
-- RECORDS EXCLUDED: None — all records retained
--   Row count reduced from 3,650 → 3,424 because duplicate
--   records for the same date-channel combination were
--   aggregated using SUM rather than dropped.
--   This preserves total spend accuracy while enforcing
--   the unique_date_channel constraint in production.
--
-- ISSUES CLEANED:
--   1. date          → Multiple formats → YYYY-MM-DD
--                      Renamed to spend_date in production
--   2. channel       → Mixed case → lowercase
--                      (EMAIL/Email/email → email)
--   3. spend_amount  → Currency symbols removed, empty → NULL
--   4. impressions   → Decimal format (1000.0) → INT,
--                      empty → NULL
--   5. clicks        → Decimal format → INT, empty → NULL
--   6. conversions   → Decimal format → INT, empty → NULL
--   7. currency      → Column dropped in production
--                      (all values were USD)
--
-- NOTE ON DUPLICATE HANDLING:
--   Raw data contained multiple rows for the same
--   date-channel combination (e.g., two rows for
--   'email' on 2023-01-15). Rather than arbitrarily
--   dropping one, records were aggregated using SUM
--   on all numeric columns to preserve total spend.
--   This mirrors how ad platform data is typically
--   reconciled in production pipelines.
-- ============================================================

USE ecommerce_marketing;

-- Autocommit turned off
SET autocommit  = 0;

-- Updates are allowed 
SET SQL_SAFE_UPDATES = 0;

-- ============================================================
-- STEP 1: INITIAL DATA EXPLORATION AND BACKUP
-- ============================================================
-- Preview raw data
SELECT * FROM stg_marketing_spend;

-- Backup staging table before any modifications
CREATE TABLE backup_stg_marketing_spend AS
 SELECT * FROM stg_marketing_spend;

-- Confirm backup row count matches source
SELECT 'channel' AS table_name, COUNT(*) AS row_count FROM stg_marketing_spend
UNION ALL
SELECT 'backup_channel' AS table_name, COUNT(*) AS row_count FROM backup_stg_marketing_spend;

-- ============================================================
-- STEP 2: CLEAN DATE COLUMN
-- ============================================================
-- Standardize multiple date formats to YYYY-MM-DD
-- Renamed to spend_date in production table
UPDATE stg_marketing_spend
SET date = CASE
 -- Already correct format (YYYY-MM-DD)
 WHEN date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
 THEN date
-- YYYY/MM/DD
 WHEN date REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
 THEN DATE_FORMAT(STR_TO_DATE(date, '%Y/%m/%d'), '%Y-%m-%d')
 -- DD-MM-YYYY
 WHEN date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
 AND CAST(SUBSTRING(date, 1, 2) AS UNSIGNED) > 12
 THEN DATE_FORMAT(STR_TO_DATE(date, '%d-%m-%Y'), '%Y-%m-%d')
-- MM-DD-YYYY
 WHEN date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
 AND CAST(SUBSTRING(date, 1, 2) AS UNSIGNED) <= 12
 THEN DATE_FORMAT(STR_TO_DATE(date, '%m-%d-%Y'), '%Y-%m-%d')
-- MM/DD/YYYY
 WHEN date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
 AND CAST(SUBSTRING(date, 1, 2) AS UNSIGNED) <= 12
 THEN DATE_FORMAT(STR_TO_DATE(date, '%m/%d/%Y'), '%Y-%m-%d')
-- DD/MM/YYYY
  WHEN date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
  THEN DATE_FORMAT(STR_TO_DATE(date,'%d/%m/%Y'), '%Y-%m-%d')
END
WHERE date IS NOT NULL;

-- ============================================================
-- STEP 3: CLEAN CHANNEL
-- ============================================================
-- Standardize to lowercase to match channels reference table
-- (EMAIL/Email/email → email)
UPDATE stg_marketing_spend
SET channel = LOWER(TRIM(channel))
WHERE channel IS NOT NULL;

-- Verify distinct channel values after standardization
SELECT channel, COUNT(*)
FROM stg_marketing_spend
GROUP BY channel;

-- ============================================================
-- STEP 4: CLEAN NUMERIC COLUMNS
-- ============================================================

-- spend_amount: empty → NULL, then strip currency symbols
UPDATE stg_marketing_spend
SET spend_amount = NULL
WHERE spend_amount = '' OR TRIM(spend_amount) = '';

UPDATE stg_marketing_spend
SET spend_amount = REGEXP_REPLACE(spend_amount, '[^0-9.]', '')
WHERE spend_amount IS NOT NULL 
  AND spend_amount REGEXP '[^0-9.]';

-- Verify no NULLs remain unexpectedly
SELECT spend_amount, COUNT(*)
FROM stg_marketing_spend
WHERE spend_amount IS NULL OR spend_amount = ''
GROUP BY spend_amount;

-- impressions: empty → NULL
-- NULL is valid for email channel (tracked in email_events)
UPDATE stg_marketing_spend
SET impressions = NULL
WHERE impressions = '' OR TRIM(impressions) = '';

-- clicks: empty → NULL
-- NULL is valid for email channel
UPDATE stg_marketing_spend
SET clicks = NULL
WHERE clicks = '' OR TRIM(clicks) = '';

-- conversions: empty → NULL
UPDATE stg_marketing_spend
SET conversions = NULL
WHERE conversions = '' OR TRIM(conversions) = '';

COMMIT;

-- ============================================================
-- STEP 5: INVESTIGATE DUPLICATE RECORDS
-- ============================================================
-- Raw data contains multiple rows for the same date-channel.
-- Strategy: aggregate using SUM rather than drop duplicates
-- to preserve total spend accuracy.

-- Show how many date-channel pairs have duplicates
SELECT date, channel, COUNT(*), SUM(COUNT(*)) 
FROM stg_marketing_spend
GROUP BY date, channel
HAVING COUNT(*) >= 2;

-- Preview the actual duplicate rows before aggregation
SELECT *
FROM stg_marketing_spend
WHERE (date, channel) IN (
    SELECT date, channel
    FROM stg_marketing_spend
    GROUP BY date, channel
    HAVING COUNT(*) >= 2
)
ORDER BY date, channel;

-- ============================================================
-- STEP 6: LOAD INTO PRODUCTION TABLE
-- ============================================================
-- Duplicates resolved by GROUP BY with SUM aggregation.
-- Decimal values cast to DECIMAL first then ROUND to INT
-- to avoid floating point precision errors.
-- currency column dropped — all values were USD.
TRUNCATE TABLE marketing_spend;

-- Insert with proper double-casting for integer columns
INSERT INTO marketing_spend (spend_date, channel, spend_amount, impressions, clicks, conversions)
SELECT 
    date AS spend_date,
    channel,
    ROUND(SUM(CAST(spend_amount AS DECIMAL(10,2))), 2) AS spend_amount,
    ROUND(SUM(CAST(impressions AS DECIMAL(15,1)))) AS impressions,    -- DECIMAL first, then rounds to INT
    ROUND(SUM(CAST(clicks AS DECIMAL(15,1)))) AS clicks,              -- DECIMAL first, then rounds to INT
    ROUND(SUM(CAST(conversions AS DECIMAL(15,1)))) AS conversions     -- DECIMAL first, then rounds to INT
FROM stg_marketing_spend
WHERE date IS NOT NULL 
  AND channel IS NOT NULL
GROUP BY date, channel;

-- ============================================================
-- STEP 7: VERIFY LOAD
-- ============================================================
-- Row count difference = duplicates aggregated (3,650 → 3,424)
SELECT 'channel' as name, COUNT(*) FROM stg_marketing_spend 
UNION ALL
SELECT 'channel' as name, COUNT(*) FROM marketing_spend;

-- ============================================================
-- STEP 8: CLEANUP
-- ============================================================
DROP TABLE backup_stg_marketing_spend;