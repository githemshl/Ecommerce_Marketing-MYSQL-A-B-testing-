-- ============================================================
-- FILE:    07_ETL_website_sessions.sql
-- ORDER:   Run after 02_ETL_customers.sql and 03_ETL_email_campaigns.sql
-- PROJECT: E-commerce Marketing ROI & Customer Acquisition
--
-- PURPOSE:
-- Cleans and transforms raw website session data from
-- stg_website_sessions and loads it into the production
-- website_sessions table.
--
-- PREREQUISITES:
--   01_Schema.sql must be run first
--   02_ETL_customers.sql must be run first (FK dependency)
--   03_ETL_email_campaigns.sql must be run first (FK dependency)
--
-- RAW SOURCE:  stg_website_sessions  (30,000 rows)
-- TARGET:      website_sessions      (30,000 rows after cleaning)
--
-- RECORDS EXCLUDED:
--   - NULL session_id
--   - Bot traffic (is_bot = 1) excluded from production
--
-- ISSUES CLEANED:
--   1. customer_id        → Decimal format → INT
--                           Orphan FKs → NULL
--                           Empty → NULL
--                           NULL retained for anonymous visitors
--   2. session_date       → Multiple formats → YYYY-MM-DD
--   3. session_start_time → Empty → NULL
--   4. traffic_source     → Mixed case → Proper case
--   5. landing_page       → Raw paths → readable labels
--                           (/home → Home, /products → Products)
--   6. pages_viewed       → Decimal format → INT, empty → NULL
--   7. device             → Mixed case → Proper case, empty → NULL
--   8. browser            → Mixed case → Proper case, empty → NULL
--   9. session_duration_seconds → Negative → NULL,
--                                 decimal → INT
--  10. conversion_value   → 0 enforced where converted = 0
--  11. is_bot             → Decimal format → INT, empty → 0
--                           Bot records excluded on load
--  12. campaign_id        → Decimal format → INT
--                           Empty → NULL
--                           NULL retained for non-email sessions
-- ============================================================

USE ecommerce_marketing;

SET AUTOCOMMIT = 0;
SET SQL_SAFE_UPDATES = 0;

-- ============================================================
-- STEP 1: INITIAL DATA EXPLORATION
-- ============================================================
-- Preview raw data and confirm schema
SELECT * FROM stg_website_sessions;

DESCRIBE stg_website_sessions;
DESCRIBE website_sessions;

-- Check for NULL session_id — required field, cannot be NULL
SELECT session_id, COUNT(*)
FROM stg_website_sessions
WHERE session_id IS NULL
GROUP BY session_id;

-- ============================================================
-- STEP 2: CLEAN CUSTOMER_ID
-- ============================================================
-- customer_id is nullable — NULL means anonymous visitor
-- Three issues to resolve:
--   1. Empty strings → NULL
--   2. Orphan FKs (customer not in production customers table) → NULL
--      These visitors signed up after the customers ETL cutoff
--      or were excluded during customers cleaning

-- Empty strings → NULL
UPDATE stg_website_sessions
SET customer_id = NULL
WHERE customer_id = '' OR TRIM(customer_id) = '';

-- Orphan customer_ids → NULL
-- Visitor exists in sessions but not in production customers table
UPDATE stg_website_sessions c
LEFT JOIN customers p ON p.customer_id = c.customer_id
SET c.customer_id = NULL
WHERE c.customer_id IS NOT NULL
  AND p.customer_id IS NULL;

COMMIT;

-- ============================================================
-- STEP 3: CLEAN SESSION_DATE
-- ============================================================
-- Standardize multiple date formats to YYYY-MM-DD
UPDATE stg_website_sessions
SET session_date = CASE
    WHEN session_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    THEN session_date
    WHEN session_date REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
    THEN DATE_FORMAT(STR_TO_DATE(session_date, '%Y/%m/%d'), '%Y-%m-%d')
    WHEN session_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
    AND CAST(SUBSTRING(session_date, 1, 2) AS UNSIGNED) > 12
    THEN DATE_FORMAT(STR_TO_DATE(session_date, '%d-%m-%Y'), '%Y-%m-%d')
    WHEN session_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
    AND CAST(SUBSTRING(session_date, 1, 2) AS UNSIGNED) <= 12
    THEN DATE_FORMAT(STR_TO_DATE(session_date, '%m/%d/%Y'), '%Y-%m-%d')
    WHEN session_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
    AND CAST(SUBSTRING(session_date, 1, 2) AS UNSIGNED) <= 12
    THEN DATE_FORMAT(STR_TO_DATE(session_date, '%m-%d-%Y'), '%Y-%m-%d')
    WHEN session_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
    THEN DATE_FORMAT(STR_TO_DATE(session_date, '%d/%m/%Y'), '%Y-%m-%d')
END;

-- ============================================================
-- STEP 4: CLEAN SESSION_START_TIME
-- ============================================================
-- Empty strings → NULL
UPDATE stg_website_sessions
SET session_start_time = NULL
WHERE session_start_time = '' OR TRIM(session_start_time) = '';

-- ============================================================
-- STEP 5: CLEAN TRAFFIC_SOURCE
-- ============================================================
-- Standardize mixed case to proper case
UPDATE stg_website_sessions
SET traffic_source = LTRIM(CONCAT(
    UPPER(LEFT(traffic_source, 1)),
    LOWER(SUBSTRING(traffic_source, 2))
))
WHERE traffic_source IS NOT NULL
  AND traffic_source != ''
  AND TRIM(traffic_source) != '';

COMMIT;

-- ============================================================
-- STEP 6: CLEAN LANDING_PAGE
-- ============================================================
-- Convert raw URL paths to readable labels
-- Unrecognized paths and empty strings → NULL
UPDATE stg_website_sessions
SET landing_page = CASE
    WHEN landing_page = '/home'                  THEN 'Home'
    WHEN landing_page = '/category/electronics'  THEN 'Electronics'
    WHEN landing_page = '/products'              THEN 'Products'
    WHEN landing_page = '/sale'                  THEN 'Sale'
    WHEN landing_page = ''                       THEN NULL
    WHEN TRIM(landing_page) = ''                 THEN NULL
END;

-- ============================================================
-- STEP 7: CLEAN PAGES_VIEWED
-- ============================================================
-- Empty → NULL, then decimal format (3.0) → integer (3)
UPDATE stg_website_sessions
SET pages_viewed = NULL
WHERE pages_viewed = '' OR TRIM(pages_viewed) = '';

UPDATE stg_website_sessions
SET pages_viewed = ROUND(pages_viewed, 0)
WHERE pages_viewed IS NOT NULL;

-- ============================================================
-- STEP 8: CLEAN DEVICE AND BROWSER
-- ============================================================
-- Both follow same pattern: empty → NULL, then proper case

-- Check distinct device values before cleaning
SELECT device, COUNT(*)
FROM stg_website_sessions
GROUP BY device;

UPDATE stg_website_sessions
SET device = NULL
WHERE device = '' OR TRIM(device) = '';

UPDATE stg_website_sessions
SET device = LTRIM(CONCAT(
    UPPER(LEFT(device, 1)),
    LOWER(SUBSTRING(device, 2))
))
WHERE device IS NOT NULL;

-- Check distinct browser values before cleaning
SELECT browser, COUNT(*)
FROM stg_website_sessions
GROUP BY browser;

UPDATE stg_website_sessions
SET browser = NULL
WHERE browser = '' OR TRIM(browser) = '';

UPDATE stg_website_sessions
SET browser = LTRIM(CONCAT(
    UPPER(LEFT(browser, 1)),
    LOWER(SUBSTRING(browser, 2))
))
WHERE browser IS NOT NULL;

-- ============================================================
-- STEP 9: CLEAN SESSION_DURATION_SECONDS
-- ============================================================
-- Check distribution before cleaning
SELECT session_duration_seconds, COUNT(*)
FROM stg_website_sessions
GROUP BY session_duration_seconds;

-- Negative values and empty strings → NULL
-- (negative duration is physically impossible)
UPDATE stg_website_sessions
SET session_duration_seconds = NULL
WHERE session_duration_seconds = ''
   OR TRIM(session_duration_seconds) = ''
   OR session_duration_seconds < 0;

-- Decimal format (312.0) → integer (312)
UPDATE stg_website_sessions
SET session_duration_seconds = FLOOR(CAST(
    session_duration_seconds AS DECIMAL(10,1)
))
WHERE session_duration_seconds IS NOT NULL;

-- ============================================================
-- STEP 10: CLEAN CONVERTED AND CONVERSION_VALUE
-- ============================================================
-- Check distinct converted values
SELECT converted, COUNT(*)
FROM stg_website_sessions
GROUP BY converted;

-- Enforce business rule: if converted = 0 then
-- conversion_value must be 0, not NULL or a leftover amount
UPDATE stg_website_sessions
SET conversion_value = '0'
WHERE converted = '0';

-- Verify conversion_value distribution after fix
SELECT conversion_value, COUNT(*)
FROM stg_website_sessions
GROUP BY conversion_value;

-- ============================================================
-- STEP 11: CLEAN IS_BOUNCE AND IS_BOT
-- ============================================================
-- Check distinct values for both flags
SELECT is_bounce, COUNT(*)
FROM stg_website_sessions
GROUP BY is_bounce;

SELECT is_bot, COUNT(*)
FROM stg_website_sessions
GROUP BY is_bot;

-- is_bot: empty strings → 0 (assume human if unknown)
UPDATE stg_website_sessions
SET is_bot = '0'
WHERE is_bot = '' OR TRIM(is_bot) = '';

-- is_bot: decimal format (1.0) → integer (1)
UPDATE stg_website_sessions
SET is_bot = FLOOR(CAST(is_bot AS DECIMAL(10,1)))
WHERE is_bot != '0';

-- ============================================================
-- STEP 12: CLEAN CAMPAIGN_ID
-- ============================================================
-- Empty strings → NULL
-- NULL retained for non-email sessions (intentional)
UPDATE stg_website_sessions
SET campaign_id = NULL
WHERE campaign_id = '' OR TRIM(campaign_id) = '';

-- Check orphan campaign_ids
-- (exist in sessions but not in email_campaigns)
SELECT DISTINCT c.campaign_id AS orphan_fk, COUNT(*) AS cnt
FROM website_sessions c
LEFT JOIN email_campaigns p ON p.campaign_id = c.campaign_id
WHERE c.campaign_id IS NOT NULL
  AND c.campaign_id != ''
  AND p.campaign_id IS NULL
GROUP BY c.campaign_id;

COMMIT;

-- ============================================================
-- STEP 13: LOAD INTO PRODUCTION TABLE
-- ============================================================
-- Bot traffic excluded: WHERE clause filters is_bot != '0'
-- customer_id and campaign_id: decimal → INT using FLOOR/CAST
-- pages_viewed: decimal → INT using FLOOR/CAST
-- NULL retained for anonymous visitors (customer_id)
-- NULL retained for non-email sessions (campaign_id)
INSERT INTO website_sessions (
    session_id,
    customer_id,
    session_date,
    session_start_time,
    traffic_source,
    landing_page,
    pages_viewed,
    session_duration_seconds,
    device,
    browser,
    converted,
    conversion_value,
    campaign_id,
    is_bounce
)
SELECT
    CAST(session_id AS UNSIGNED),
    CASE
        WHEN customer_id IS NOT NULL
         AND customer_id REGEXP '^[0-9]+(\\.[0-9]+)?$'
        THEN FLOOR(CAST(customer_id AS DECIMAL(10,1)))
    END,
    CAST(session_date AS DATE),
    CAST(session_start_time AS TIME),
    traffic_source,
    landing_page,
    CASE
        WHEN pages_viewed IS NOT NULL
         AND pages_viewed REGEXP '^[0-9]+(\\.[0-9]+)?$'
        THEN FLOOR(CAST(pages_viewed AS DECIMAL(10,1)))
    END,
    CAST(session_duration_seconds AS UNSIGNED),
    device,
    browser,
    CAST(converted AS UNSIGNED),
    CAST(conversion_value AS DECIMAL(10,2)),
    CASE
        WHEN campaign_id IS NOT NULL
         AND campaign_id REGEXP '^[0-9]+(\\.[0-9]+)?$'
        THEN FLOOR(CAST(campaign_id AS DECIMAL(10,1)))
    END,
    CAST(is_bounce AS UNSIGNED)
FROM stg_website_sessions
WHERE (is_bot = '0' OR is_bot IS NULL)
  AND session_id IS NOT NULL;

COMMIT;

-- ============================================================
-- STEP 14: VERIFY LOAD
-- ============================================================
SELECT
    (SELECT COUNT(*) FROM website_sessions)    AS production_rows,
    (SELECT COUNT(*) FROM stg_website_sessions
      WHERE (is_bot = '0' OR is_bot IS NULL)
        AND session_id IS NOT NULL)            AS expected_rows;