/* ================================================================================
   03_Financial_Metrics_CAC_LTV_ROAS.sql
   ================================================================================
   
   Purpose: Evaluate marketing efficiency and customer value by channel
   
   Business Questions:
   • Which channel acquires customers most efficiently? (CAC)
   • Which customers are most valuable? (LTV)
   • Is acquisition cost justified by customer value? (LTV:CAC)
   • How much revenue does each marketing dollar generate? (ROAS)
   
   Skills Demonstrated:
   • Multiple CTEs
   • LEFT/RIGHT JOINs
   • NULLIF for safe division
   • Financial metric calculations
   
   ================================================================================
*/

USE ecommerce_marketing;

/* ----------------------------------------------------------------------------
   3.1 CUSTOMER ACQUISITION COST (CAC) BY CHANNEL
   Formula: CAC = Total Marketing Spend / Number of New Customers Acquired
   ---------------------------------------------------------------------------- */

WITH customers_metrics AS (
    SELECT 
        acquisition_source AS channel, 
        COUNT(DISTINCT customer_id) AS customers_per_source, 
        COUNT(CASE WHEN signup_date BETWEEN '2023-01-01' AND '2024-12-30' THEN 1 ELSE NULL END) AS new_customers
    FROM customers
    GROUP BY acquisition_source
),
marketing_metrics AS (
    SELECT 
        channel, 
        SUM(spend_amount) AS total_spend
    FROM marketing_spend 
    GROUP BY channel
)
SELECT 
    c.channel, 
    m.total_spend, 
    c.customers_per_source, 
    c.new_customers, 
    ROUND(m.total_spend / c.new_customers, 2) AS CAC
FROM customers_metrics c
INNER JOIN marketing_metrics m ON c.channel = m.channel;

/* RESULTS:
   Channel       Spend         Customers  New_Customers  CAC
   Email         40,020.53     733        731            54.75
   Paid_search   159,080.76    590        590            269.63
   Social_media  120,416.01    446        445            270.60
   
   INTERPRETATION:
   • Email has lowest CAC at $54.75 per customer
   • Paid Search and Social Media ~$270 per customer (5x higher than email)
   • Email is the most cost-efficient paid acquisition channel
*/


/* ----------------------------------------------------------------------------
   3.2 CAC FOR ALL CHANNELS (Including organic with $0 spend)
   ---------------------------------------------------------------------------- */

WITH marketing_metrics AS (
    SELECT 
        c.channel_name, 
        SUM(m.spend_amount) AS total_spend
    FROM channels c
    LEFT JOIN marketing_spend m ON c.channel_name = m.channel 
    GROUP BY c.channel_name
),
customer_metrics AS (
    SELECT 
        c.channel_name, 
        COUNT(DISTINCT u.customer_id) AS total_customers
    FROM customers u
    RIGHT JOIN channels c ON c.channel_name = u.acquisition_source 
        AND u.signup_date BETWEEN '2023-01-01' AND '2024-12-30'
    GROUP BY c.channel_name
)
SELECT 
    c.channel_name, 
    m.total_spend, 
    c.total_customers, 
    ROUND(m.total_spend / NULLIF(c.total_customers, 0), 2) AS CAC
FROM marketing_metrics m 
INNER JOIN customer_metrics c ON c.channel_name = m.channel_name;

/* RESULTS:
   Channel         Spend         Customers  CAC
   Affiliate       59,960.66     0          NULL
   Direct          NULL          273        NULL
   Display_ads     80,071.34     0          NULL
   Organic         NULL          0          NULL
   Organic_search  NULL          595        NULL
   Referral        NULL          315        NULL
   Social          NULL          0          NULL
   Email           40,020.53     731        54.75
   Paid_search     159,080.76    590        269.63
   Social_media    120,416.01    445        270.60
   
   INTERPRETATION:
   • Direct, Organic_search, Referral acquire customers with $0 spend
   • 40% of customers (1,183) come from free organic channels
   • Affiliate and Display_ads spent $140K but no direct signups attributed
   • Email is the most efficient paid channel at $54.75 CAC
*/


/* ----------------------------------------------------------------------------
   3.3 CUSTOMER LIFETIME VALUE (LTV) BY ACQUISITION SOURCE
   Formula: LTV = Total Revenue / Number of Customers
   ---------------------------------------------------------------------------- */

SELECT 
    c.acquisition_source, 
    COUNT(DISTINCT c.customer_id) AS customers,
    COUNT(t.transaction_id) AS transactions,
    SUM(t.revenue) AS total_revenue,
    ROUND(SUM(t.revenue) / COUNT(t.transaction_id), 2) AS AOV,
    ROUND(COUNT(t.transaction_id) / COUNT(DISTINCT c.customer_id), 2) AS purchase_freq,
    ROUND(SUM(t.revenue) / COUNT(DISTINCT c.customer_id), 2) AS LTV
FROM customers c
INNER JOIN transactions t ON c.customer_id = t.customer_id
WHERE c.signup_date BETWEEN '2023-01-01' AND '2024-12-30'
GROUP BY c.acquisition_source
ORDER BY LTV DESC;

/* RESULTS:
   Channel         Customers  Trans   Revenue      AOV     Freq  LTV
   Email           694        2,205   552,367.14   250.51  3.18  795.92
   Organic_search  550        1,766   432,057.64   244.65  3.21  785.56
   Social_media    429        1,391   336,667.37   242.03  3.24  784.77
   Paid_search     568        1,788   437,229.86   244.54  3.15  769.77
   Referral        305        964     230,799.29   239.42  3.16  756.72
   Direct          258        769     189,323.91   246.19  2.98  733.81
   
   INTERPRETATION:
   • Email customers have highest LTV at $795.92
   • LTV range is narrow ($733.81 to $795.92) - only 8.5% variance
   • Purchase frequency is consistent across channels (2.98 to 3.24)
   • AOV is similar across channels ($239-$250)
   • Direct channel has lowest LTV ($733.81) and lowest frequency (2.98)
*/


/* ----------------------------------------------------------------------------
   3.4 LTV:CAC RATIO BY CHANNEL
   Formula: LTV:CAC = Customer Lifetime Value / Customer Acquisition Cost
   ---------------------------------------------------------------------------- */

WITH ltv_metrics AS (
    SELECT 
        c.acquisition_source AS channel, 
        COUNT(DISTINCT c.customer_id) AS customers,
        COUNT(t.transaction_id) AS transactions, 
        ROUND(SUM(t.revenue) / COUNT(DISTINCT c.customer_id), 2) AS LTV
    FROM customers c
    INNER JOIN transactions t ON c.customer_id = t.customer_id
    WHERE c.signup_date BETWEEN '2023-01-01' AND '2024-12-30'
    GROUP BY c.acquisition_source
),
marketing_metrics AS (
    SELECT 
        c.channel_name, 
        SUM(m.spend_amount) AS total_spend
    FROM channels c
    LEFT JOIN marketing_spend m ON c.channel_name = m.channel 
    GROUP BY c.channel_name
),
cac_metrics AS (
    SELECT 
        m.channel_name, 
        ROUND(m.total_spend / NULLIF(l.customers, 0), 2) AS CAC
    FROM ltv_metrics l
    INNER JOIN marketing_metrics m ON l.channel = m.channel_name
    GROUP BY m.channel_name
)
SELECT 
    l.channel, 
    l.LTV, 
    c.CAC, 
    ROUND(l.LTV / NULLIF(c.CAC, 0), 2) AS LTV_to_CAC
FROM ltv_metrics l
INNER JOIN cac_metrics c ON c.channel_name = l.channel;

/* RESULTS:
   Channel       LTV      CAC      LTV:CAC
   Direct        733.81   NULL     NULL
   Email         795.92   57.67    13.80
   Organic       785.56   NULL     NULL
   Paid_search   769.77   280.07   2.75
   Referral      756.72   NULL     NULL
   Social_media  784.77   280.69   2.80
   
   INTERPRETATION:
   • Email: 13.80x ratio - exceptional ROI (well above 3.0 benchmark)
   • Paid Search: 2.75x ratio - below 3.0 healthy threshold
   • Social Media: 2.80x ratio - below 3.0 healthy threshold
   • Organic channels have infinite ratio (no acquisition cost)
   
   INDUSTRY BENCHMARKS:
   • LTV:CAC < 1.0 = Losing money on acquisition
   • LTV:CAC 1.0-3.0 = Breakeven to moderate
   • LTV:CAC > 3.0 = Healthy
   • LTV:CAC > 5.0 = May be underinvesting in growth
*/


/* ----------------------------------------------------------------------------
   3.5 RETURN ON AD SPEND (ROAS) BY CHANNEL
   Formula: ROAS = Revenue / Marketing Spend
   ---------------------------------------------------------------------------- */

WITH transaction_metrics AS (
    SELECT 
        c.channel_name, 
        SUM(t.revenue) AS total_revenue
    FROM transactions t
    RIGHT JOIN channels c ON c.channel_name = t.source_channel 
        AND t.transaction_date BETWEEN '2023-01-01' AND '2024-12-30'
    GROUP BY c.channel_name
),
marketing AS (
    SELECT 
        c.channel_name, 
        SUM(m.spend_amount) AS total_spend
    FROM marketing_spend m
    RIGHT JOIN channels c ON c.channel_name = m.channel
        AND m.spend_date BETWEEN '2023-01-01' AND '2024-12-30'
    GROUP BY c.channel_name
)
SELECT 
    t.channel_name, 
    t.total_revenue, 
    m.total_spend, 
    ROUND(t.total_revenue / NULLIF(m.total_spend, 0), 2) AS ROAS
FROM transaction_metrics t
INNER JOIN marketing m ON t.channel_name = m.channel_name;

/* RESULTS:
   Channel         Revenue      Spend         ROAS
   Email           800,558.31   40,020.53     20.00
   Social_media    274,634.80   120,416.01    2.28
   Paid_search     277,272.97   159,080.76    1.74
   Affiliate       NULL         59,960.66     NULL
   Direct          266,836.01   NULL          NULL
   Display_ads     NULL         80,071.34     NULL
   Organic_search  282,632.67   NULL          NULL
   Referral        NULL         NULL          NULL
   
   INTERPRETATION:
   • Email: 20.0x ROAS - generates $20 for every $1 spent (exceptional)
   • Social Media: 2.28x ROAS - generates $2.28 per $1 spent
   • Paid Search: 1.74x ROAS - generates $1.74 per $1 spent
   • Display & Affiliate: No attributed revenue despite $140K spend
   • Organic channels generate revenue with no direct spend
*/


/* ================================================================================
   FINANCIAL METRICS SUMMARY
   ================================================================================
   
   CUSTOMER ACQUISITION COST (CAC):
   • Email: $54.75 (most efficient)
   • Paid Search: $269.63 (5x higher)
   • Social Media: $270.60 (5x higher)
   • 40% of customers acquired for free (organic channels)
   
   CUSTOMER LIFETIME VALUE (LTV):
   • Email: $795.92 (highest)
   • Range: $733.81 to $795.92 (only 8.5% variance)
   • Consistent AOV and frequency across channels
   
   LTV:CAC RATIO:
   • Email: 13.80x (exceptional)
   • Paid Search: 2.75x (below 3.0 benchmark)
   • Social Media: 2.80x (below 3.0 benchmark)
   
   ROAS:
   • Email: 20.0x (exceptional)
   • Social Media: 2.28x
   • Paid Search: 1.74x
   
   KEY INSIGHT:
   Email marketing significantly outperforms all other paid channels
   across every financial metric (CAC, LTV, LTV:CAC, ROAS).
   
   ================================================================================
*/
