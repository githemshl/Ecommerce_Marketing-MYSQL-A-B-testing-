-- ============================================================
-- FILE:    03_ETL_email_campaigns.sql
-- ORDER:   Run after 01_Schema.sql
-- PROJECT: E-commerce Marketing ROI & Customer Acquisition
--
-- PURPOSE:
-- Cleans and transforms raw email campaign data from
-- stg_email_campaigns and loads it into the production
-- email_campaigns table.
--
-- PREREQUISITE: 01_Schema.sql must be run first
--
-- RAW SOURCE:  stg_email_campaigns  (48 rows)
-- TARGET:      email_campaigns      (48 rows after cleaning)
--
-- RECORDS EXCLUDED: None
--   All 48 campaigns were valid and retained.
--
-- ISSUES CLEANED:
--   1. subject_line  → Character encoding artifact removed
--                      (Ã°Å¸â€Â¥ → 🔥 emoji was corrupted on import)
--   2. send_date     → Multiple formats → YYYY-MM-DD
--   3. send_time     → 12hr and 24hr mixed → HH:MM:SS (24hr)
--   4. campaign_type → Mixed case → Proper case
--   5. discount_offered → '10%'/'10 percent'/10 → integer 0-25
--                         Renamed to discount_percent in production
--   6. marketing_cost → Rounded to 2 decimal places
--
-- NOTE ON COLUMN RENAME:
--   Raw column 'discount_offered' (VARCHAR) was renamed to
--   'discount_percent' (INT) in the production table to better
--   reflect the cleaned data type and business meaning.
-- ============================================================

USE ecommerce_marketing;

-- Updates are allowed 
SET SQL_SAFE_UPDATES =0;

-- Autocommit turned off
SET autocommit = 0;

-- ============================================================
-- STEP 1: INITIAL DATA EXPLORATION
-- ============================================================
-- Check raw data and identify quality issues before cleaning
-- Preview raw data
SELECT * FROM stg_email_campaigns;

-- Check for duplicate campaign IDs
SELECT campaign_id, COUNT(*) AS count
FROM stg_email_campaigns
GROUP BY campaign_id
HAVING COUNT(*) > 1;

-- Check for missing campaign_name or variant
-- These are critical fields for A/B test analysis
SELECT campaign_name, variant
FROM stg_email_campaigns
WHERE campaign_name IS NULL OR TRIM(campaign_name) = '' 
   OR variant IS NULL OR TRIM(variant) = '';

-- ============================================================
-- STEP 2: CLEAN SUBJECT_LINE
-- ============================================================
-- Remove character encoding artifact from emoji import
-- Root cause: 🔥 emoji in subject line was corrupted during
-- CSV import due to UTF-8 encoding mismatch
UPDATE stg_email_campaigns 
SET subject_line = LTRIM(REPLACE(subject_line, 'ðŸ”¥' , ''))
 WHERE subject_line LIKE '%ðŸ”¥%';

-- Verify subject lines look correct after fix
SELECT subject_line, COUNT(*)
FROM stg_email_campaigns
GROUP BY subject_line;
 
COMMIT;

-- ============================================================
-- STEP 3: CLEAN SEND_DATE
-- ============================================================
-- Standardize multiple date formats to YYYY-MM-DD
UPDATE stg_email_campaigns
SET send_date = CASE 
 -- Already correct format (YYYY-MM-DD)
  WHEN send_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  THEN send_date
 -- YYYY/MM/DD
  WHEN send_date REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
  THEN DATE_FORMAT(STR_TO_DATE(send_date, '%Y/%m/%d') , '%Y-%m-%d')
-- DD-MM-YYYY
  WHEN send_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
   AND CAST(SUBSTRING(send_date, 1 ,2) AS UNSIGNED) >12
  THEN DATE_FORMAT(STR_TO_DATE(send_date, '%d-%m-%Y'), '%Y-%m-%d')
-- MM/DD/YYYY
  WHEN send_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
   AND CAST(SUBSTRING(send_date ,1 ,2)AS UNSIGNED) <= 12
  THEN DATE_FORMAT(STR_TO_DATE(send_date,'%m/%d/%Y'), '%Y-%m-%d')  
-- MM-DD-YYYY
  WHEN send_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
   AND CAST(SUBSTRING(send_date,1,2)AS UNSIGNED) <= 12
  THEN DATE_FORMAT(STR_TO_DATE(send_date,'%m-%d-%Y'), '%Y-%m-%d')
 -- DD/MM/YYYY
  WHEN send_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
  THEN DATE_FORMAT(STR_TO_DATE(send_date,'%d/%m/%Y'), '%Y-%m-%d')
-- Month DD, YYYY (December 25, 2025)
  WHEN send_date REGEXP '^[A-Za-z]+ [0-9]{1,2}, [0-9]{4}$'
  THEN DATE_FORMAT(STR_TO_DATE(send_date, '%M %d, %Y'), '%Y-%m-%d')
-- DD Month YYYY (25 December 2025)
  WHEN send_date REGEXP '^[0-9]{1,2} [A-Za-z]+ [0-9]{4}$'
  THEN DATE_FORMAT(STR_TO_DATE(send_date, '%d %M %Y'), '%Y-%m-%d')
END;

-- Verify: any NULLs after date conversion indicate unrecognized formats
SELECT send_date, campaign_id
FROM stg_email_campaigns
 WHERE send_date IS NULL;

COMMIT;

-- ============================================================
-- STEP 4: CLEAN SEND_TIME
-- ============================================================
-- Standardize 12hr and 24hr mixed formats to HH:MM (24hr)
UPDATE stg_email_campaigns
SET send_time = CASE
-- Already 24-hour format (HH:MM)
 WHEN send_time REGEXP '^[0-9]{2}:[0-9]{2}$'
 THEN send_time
-- 24-hour format with second (HH:MM:SS)
 WHEN send_time REGEXP '^[0-9]{2}:[0-9]{2}:[0-9]{2}$'
 THEN SUBSTRING(send_time, 1, 5)
-- 12-hour format (H:MM AM/PM or HH:MM AM/PM)
 WHEN send_time REGEXP '^[0-9]{1,2}:[0-9]{2} [AP]M$'
 THEN TIME_FORMAT(STR_TO_DATE(send_time, '%h:%i %p'), '%H:%i')
 ELSE NULL
END;

COMMIT;

-- ============================================================
-- STEP 5: CLEAN CAMPAIGN_TYPE
-- ============================================================
-- Check distinct values before standardizing
SELECT campaign_type, count(*)
FROM stg_email_campaigns 
GROUP BY campaign_type;

-- Standardize mixed case to proper case
-- (PROMOTIONAL -> Promotional, newsletter -> Newsletter)
UPDATE stg_email_campaigns 
SET campaign_type = LTRIM(CONCAT(UPPER(LEFT(campaign_type, 1)), LOWER(SUBSTRING(campaign_type, 2))))
WHERE campaign_type IS NOT NULL;

COMMIT;

-- ============================================================
-- STEP 6: CLEAN DISCOUNT_OFFERED AND MARKETING_COST
-- ============================================================
-- Standardize discount format: '10%' / '10 percent' / 10 → integer
-- NULL means no discount was offered → default to 0
UPDATE stg_email_campaigns
SET discount_offered = CASE
  WHEN discount_offered IN ('25', '25%', '25 percent') THEN '25'
  WHEN discount_offered IN ('10', '10%', '10 percent') THEN '10'
  WHEN discount_offered IN ('15', '15%', '15 percent') THEN '15'
  WHEN discount_offered IN ('20', '20%', '20 percent') THEN '20'
  ELSE NULL 
END;

-- Round marketing cost to 2 decimal places
UPDATE stg_email_campaigns
SET marketing_cost = ROUND(marketing_cost, 0)
WHERE marketing_cost IS NOT NULL;

-- Default NULL discount to 0 (no discount = 0%, not missing data)
UPDATE stg_email_campaigns
SET discount_offered = 0
 WHERE discount_offered IS NULL;

COMMIT;

-- ============================================================
-- STEP 7: LOAD INTO PRODUCTION TABLE
-- ============================================================
INSERT INTO email_campaigns 
SELECT * FROM stg_email_campaigns;

-- ============================================================
-- STEP 8: VERIFY LOAD
-- ============================================================
SELECT  COUNT(*) FROM email_campaigns;