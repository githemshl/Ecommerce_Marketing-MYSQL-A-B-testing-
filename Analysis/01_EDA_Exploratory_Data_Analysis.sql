/* ================================================================================
   01_EDA_Exploratory_Data_Analysis.sql
   ================================================================================
   
   Purpose: Understand data structure, volume, quality, and distributions
   
   Skills Demonstrated:
   • UNION ALL for combining results
   • COUNT, MIN, MAX, AVG, STD for statistics
   • CASE WHEN for conditional counting
   • Window Functions for percentage calculations
   • GROUP BY for aggregations
   
   ================================================================================
*/

USE ecommerce_marketing;

/* ----------------------------------------------------------------------------
   1.1 DATA INVENTORY: Record Counts Across All Tables
   ---------------------------------------------------------------------------- */

SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'email_campaigns', COUNT(*) FROM email_campaigns
UNION ALL
SELECT 'email_events', COUNT(*) FROM email_events
UNION ALL
SELECT 'marketing_spend', COUNT(*) FROM marketing_spend
UNION ALL 
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL 
SELECT 'website_sessions', COUNT(*) FROM website_sessions
UNION ALL
SELECT 'channels', COUNT(*) FROM channels;

/* RESULTS:
   customers         2,952
   email_campaigns      48
   email_events     85,223
   marketing_spend   3,424
   transactions      8,892
   website_sessions 30,000
   channels             10
   
   INTERPRETATION:
   • email_events is the largest table (85K records) - one row per email sent
   • 48 email campaigns over 2 years = ~2 campaigns per month
   • 8,892 transactions from 2,952 customers = ~3 orders per customer average
*/


/* ----------------------------------------------------------------------------
   1.2 DATE RANGE VALIDATION
   ---------------------------------------------------------------------------- */

SELECT 
    'email_campaigns' AS data_source,
    MIN(send_date) AS start_date, 
    MAX(send_date) AS end_date,
    DATEDIFF(MAX(send_date), MIN(send_date)) AS days_span
FROM email_campaigns
UNION ALL
SELECT 
    'transactions',
    MIN(transaction_date), 
    MAX(transaction_date),
    DATEDIFF(MAX(transaction_date), MIN(transaction_date))
FROM transactions
UNION ALL
SELECT 
    'marketing_spend',
    MIN(spend_date), 
    MAX(spend_date),
    DATEDIFF(MAX(spend_date), MIN(spend_date))
FROM marketing_spend;

/* RESULTS:
   email_campaigns  2023-01-01  2024-11-21  690
   transactions     2023-01-01  2024-12-31  730
   marketing_spend  2023-01-01  2024-12-30  729
   
   INTERPRETATION:
   • All datasets start on Jan 1, 2023 - aligned for analysis
   • Transactions and marketing spend cover full 2 years (730 days)
   • Email campaigns end Nov 21, 2024 - 40 days less coverage
*/


/* ----------------------------------------------------------------------------
   1.3 DATA COMPLETENESS: Null Count Analysis
   ---------------------------------------------------------------------------- */

SELECT 
    'customers' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
    SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_first_name,
    SUM(CASE WHEN age IS NULL OR age = 0 THEN 1 ELSE 0 END) AS null_or_zero_age,
    SUM(CASE WHEN gender IS NULL OR gender = '' THEN 1 ELSE 0 END) AS null_gender,
    SUM(CASE WHEN acquisition_source IS NULL THEN 1 ELSE 0 END) AS null_acquisition
FROM customers;

/* RESULTS:
   customers  2952  0  0  1763  607  0
   
   INTERPRETATION:
   • Email and first_name are 100% complete - core identifiers reliable
   • 60% (1,763) have missing/zero age - age-based analysis limited
   • 21% (607) have missing gender - gender analysis limited
   • Acquisition source is 100% complete - channel attribution reliable
*/


/* ----------------------------------------------------------------------------
   1.4 CARDINALITY CHECK: Distinct Values in Categorical Columns
   ---------------------------------------------------------------------------- */

SELECT 'acquisition_source' AS column_name, COUNT(DISTINCT acquisition_source) AS unique_values FROM customers
UNION ALL
SELECT 'customer_segment', COUNT(DISTINCT customer_segment) FROM customers
UNION ALL
SELECT 'source_channel', COUNT(DISTINCT source_channel) FROM transactions
UNION ALL
SELECT 'order_status', COUNT(DISTINCT order_status) FROM transactions
UNION ALL
SELECT 'payment_method', COUNT(DISTINCT payment_method) FROM transactions
UNION ALL
SELECT 'product_category', COUNT(DISTINCT product_category) FROM transactions
UNION ALL
SELECT 'sent_status', COUNT(DISTINCT sent_status) FROM email_events
UNION ALL
SELECT 'device_type', COUNT(DISTINCT device_type) FROM email_events
UNION ALL
SELECT 'campaign_type', COUNT(DISTINCT campaign_type) FROM email_campaigns
UNION ALL
SELECT 'variant', COUNT(DISTINCT variant) FROM email_campaigns;

/* RESULTS:
   acquisition_source  6
   customer_segment    4
   source_channel      5
   order_status        4
   payment_method      3
   product_category    3
   sent_status         3
   device_type         4
   campaign_type       3
   variant             2
   
   INTERPRETATION:
   • Low cardinality across all fields - data is well-structured for grouping
   • 6 acquisition channels available for customer attribution
   • 2 variants confirm A/B test design
*/


/* ----------------------------------------------------------------------------
   1.5 REVENUE DISTRIBUTION STATISTICS
   ---------------------------------------------------------------------------- */

SELECT 
    COUNT(*) AS total_transactions,
    ROUND(MIN(revenue), 2) AS min_revenue,
    ROUND(MAX(revenue), 2) AS max_revenue,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    ROUND(STD(revenue), 2) AS std_revenue
FROM transactions;

/* RESULTS:
   8892  0.00  499.96  254.93  140.28
   
   INTERPRETATION:
   • Average order value is $254.93
   • Revenue ranges from $0 to $499.96
   • $0 transactions exist (possibly refunds or free items)
   • Standard deviation $140.28 (~55% of mean) - moderate spread
*/


/* ----------------------------------------------------------------------------
   1.6 ORDER STATUS DISTRIBUTION
   ---------------------------------------------------------------------------- */

SELECT 
    order_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM transactions
GROUP BY order_status
ORDER BY count DESC;

/* RESULTS:
   Completed   4462  50.18
   Refunded    1484  16.69
   Cancelled   1477  16.61
   Pending     1469  16.52
   
   INTERPRETATION:
   • 50.18% of transactions complete successfully
   • Refund and cancellation rates are similar (~16.6% each)
   • 16.52% pending may need follow-up
   • Combined non-completed rate is ~50%
*/


/* ----------------------------------------------------------------------------
   1.7 EMAIL DELIVERY STATUS DISTRIBUTION
   ---------------------------------------------------------------------------- */

SELECT 
    sent_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM email_events
GROUP BY sent_status
ORDER BY count DESC;

/* RESULTS:
   Delivered  49642  58.25
   Bounced    18835  22.10
   Failed     16746  19.65
   
   INTERPRETATION:
   • 58.25% delivery rate - just over half reach recipients
   • 22.10% bounce rate - suggests email list quality issues
   • 19.65% failure rate - technical delivery issues
   • Combined non-delivery rate is 41.75%
*/


/* ----------------------------------------------------------------------------
   1.8 DUPLICATE CHECK
   ---------------------------------------------------------------------------- */

SELECT 
    'transactions' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_id) AS unique_ids,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicates
FROM transactions
UNION ALL
SELECT 'customers', COUNT(*), COUNT(DISTINCT customer_id), COUNT(*) - COUNT(DISTINCT customer_id)
FROM customers
UNION ALL
SELECT 'email_events', COUNT(*), COUNT(DISTINCT event_id), COUNT(*) - COUNT(DISTINCT event_id)
FROM email_events;

/* RESULTS:
   transactions  8892   8892   0
   customers     2952   2952   0
   email_events  85223  85223  0
   
   INTERPRETATION:
   • Zero duplicates across all three key tables
   • Data integrity is maintained
*/


/* ----------------------------------------------------------------------------
   1.9 A/B TEST BALANCE CHECK
   ---------------------------------------------------------------------------- */

SELECT 
    variant, 
    COUNT(DISTINCT campaign_id) AS campaign_count
FROM email_campaigns
GROUP BY variant;

/* RESULTS:
   A  24
   B  24
   
   INTERPRETATION:
   • Perfectly balanced (24 campaigns each)
   • Valid for fair A/B comparison
*/


/* ----------------------------------------------------------------------------
   1.10 CUSTOMER DISTRIBUTION BY ACQUISITION CHANNEL
   ---------------------------------------------------------------------------- */

SELECT 
    acquisition_source, 
    COUNT(DISTINCT customer_id) AS customer_count,
    ROUND(COUNT(DISTINCT customer_id) * 100.0 / 
          (SELECT COUNT(*) FROM customers), 2) AS pct_of_total
FROM customers
GROUP BY acquisition_source
ORDER BY customer_count DESC;

/* RESULTS:
   Email           733  24.83
   Organic_search  595  20.16
   Paid_search     590  19.99
   Social_media    446  15.11
   Referral        315  10.67
   Direct          273   9.25
   
   INTERPRETATION:
   • Email is the largest acquisition channel at 24.83%
   • Organic channels (organic + referral + direct) total 40.08%
   • Paid channels (paid_search + social_media) total 35.10%
*/


/* ----------------------------------------------------------------------------
   1.11 TOTAL REVENUE AND TRANSACTION COUNT
   ---------------------------------------------------------------------------- */

SELECT 
    SUM(revenue) AS total_revenue, 
    COUNT(DISTINCT transaction_id) AS transaction_cnt
FROM transactions;

/* RESULTS:
   2179912.71  8892
   
   INTERPRETATION:
   • Total revenue over 2 years: $2,179,912.71
   • 8,892 total transactions
   • Average transaction value: $245.14
*/


/* ----------------------------------------------------------------------------
   1.12 EMAIL CAMPAIGNS VS OTHER SOURCES
   ---------------------------------------------------------------------------- */

SELECT 
    CASE
        WHEN campaign_id IS NOT NULL THEN 'Email campaigns'
        WHEN campaign_id IS NULL THEN 'Other sources'
    END AS Sources,
    COUNT(*) AS transaction_cnt,
    SUM(revenue) AS total_revenue
FROM transactions
GROUP BY CASE 
        WHEN campaign_id IS NOT NULL THEN 'Email campaigns'
        WHEN campaign_id IS NULL THEN 'Other sources'
    END;

/* RESULTS:
   Other sources     5378  1318967.02
   Email campaigns   3514   860945.69
   
   INTERPRETATION:
   • 39.5% of transactions came from email campaigns
   • 60.5% came from other sources
   • Email campaigns generated $860,946 (39.5% of revenue)
*/


/* ================================================================================
   EDA SUMMARY
   ================================================================================
   
   DATA QUALITY:
   • Core fields (email, customer_id) are 100% complete
   • Age (60%) and gender (21%) have missing values
   • Zero duplicates in key tables
   
   DATA VOLUME:
   • 2 years of data (Jan 2023 - Dec 2024)
   • 2,952 customers, 8,892 transactions, 85,223 email events
   • Total revenue: $2,179,912.71
   
   KEY OBSERVATIONS:
   • 50% of transactions complete successfully
   • 58% email delivery rate (42% bounce/fail)
   • Average order value: $254.93
   • A/B test is balanced (24 campaigns each variant)
   • 40% of customers acquired through free organic channels
   
   ================================================================================
*/
