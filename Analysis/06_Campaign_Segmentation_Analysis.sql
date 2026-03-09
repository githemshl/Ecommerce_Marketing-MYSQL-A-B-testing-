/* ================================================================================
   06_Campaign_Segmentation_Analysis.sql
   ================================================================================
   
   Purpose: Analyze individual campaign performance and customer segment behavior
   
   Business Questions:
   • Which specific campaigns generated the most revenue?
   • Which campaigns had the best engagement?
   • How do customer segments differ in purchasing behavior?
   
   Skills Demonstrated:
   • Multiple CTEs for complex aggregations
   • JOIN for combining campaign and transaction data
   • GROUP BY with multiple columns
   • ORDER BY for ranking
   
   ================================================================================
*/

USE ecommerce_marketing;

/* ----------------------------------------------------------------------------
   6.1 CAMPAIGN PERFORMANCE RANKING
   ---------------------------------------------------------------------------- */

WITH campaign_engagement AS (
    SELECT
        c.campaign_id,
        c.campaign_name,
        c.variant,
        c.send_date,
        SUM(CASE WHEN e.sent_status = 'Delivered' THEN 1 ELSE 0 END) AS delivered,
        SUM(CASE WHEN e.opened = 1 THEN 1 ELSE 0 END) AS opened,
        SUM(CASE WHEN e.clicked = 1 THEN 1 ELSE 0 END) AS clicked
    FROM email_campaigns c
    JOIN email_events e ON c.campaign_id = e.campaign_id
    GROUP BY c.campaign_id, c.campaign_name, c.variant, c.send_date
),
campaign_revenue AS (
    SELECT
        c.campaign_id,
        c.campaign_name,
        c.variant,
        c.send_date,
        SUM(CASE WHEN t.order_status = 'Completed' 
                  AND t.source_channel = 'Email' THEN 1 ELSE 0 END) AS purchases,
        SUM(t.revenue) AS revenue
    FROM email_campaigns c
    JOIN transactions t ON c.campaign_id = t.campaign_id
    GROUP BY c.campaign_id, c.campaign_name, c.variant, c.send_date
)
SELECT   
    e.campaign_id,
    e.campaign_name,
    e.variant,
    e.send_date,
    e.delivered,
    ROUND(e.opened / e.delivered * 100, 2) AS open_rate,
    ROUND(e.clicked / e.opened * 100, 2) AS click_rate,
    r.purchases,
    r.revenue,
    ROUND(r.revenue / e.delivered) AS revenue_per_email
FROM campaign_engagement e
JOIN campaign_revenue r ON e.campaign_id = r.campaign_id
ORDER BY r.revenue DESC;

/* RESULTS (Top 10 by Revenue):
   ID    Campaign_Name            Variant  Send_Date   Delivered  Open%  Click%  Purchases  Revenue      Rev/Email
   1024  Monthly_Promo_Dec2023_A  A        2023-12-27  1,018      19.00  21.00   24         24,150.53    24
   1008  Monthly_Promo_May2023_A  A        2023-05-01  1,188      16.00  27.00   17         23,467.41    20
   1031  Monthly_Promo_Mar2024_B  B        2024-03-26  692        17.00  28.00   16         21,245.90    31
   1004  Monthly_Promo_Mar2023_A  A        2023-02-03  778        20.00  25.00   11         20,966.85    27
   1016  Monthly_Promo_Aug2023_A  A        2023-08-29  852        14.00  32.00   10         20,832.99    24
   1012  Monthly_Promo_Jun2023_A  A        2023-06-30  1,138      13.00  33.00   10         20,787.36    18
   1027  Monthly_Promo_Jan2024_B  B        2024-01-26  985        22.00  32.00   14         20,633.66    21
   1030  Monthly_Promo_Mar2024_A  A        2024-03-26  977        16.00  18.00   14         20,415.26    21
   1000  Monthly_Promo_Jan2023_A  A        2023-01-01  810        14.00  36.00   14         20,382.34    25
   1007  Monthly_Promo_Apr2023_B  B        2023-04-01  1,204      21.00  27.00   14         19,957.89    17
   
   INTERPRETATION:
   
   TOP PERFORMERS BY REVENUE:
   • Dec 2023 A: $24,151 (highest revenue)
   • May 2023 A: $23,467
   • Mar 2024 B: $21,246
   
   TOP PERFORMERS BY EFFICIENCY (Revenue per Email):
   • Mar 2024 B: $31 per email (best efficiency)
   • Mar 2023 A: $27 per email
   • Jan 2023 A: $25 per email
   
   OBSERVATIONS:
   • Variant A campaigns dominate top revenue spots (7 of top 10)
   • Variant B has higher open rates (17-22%) vs Variant A (13-20%)
   • Mar 2024 B has highest efficiency despite lower volume
   • Holiday campaign (Dec 2023 A) generated highest total revenue
*/


/* ----------------------------------------------------------------------------
   6.2 CUSTOMER SEGMENTATION ANALYSIS
   ---------------------------------------------------------------------------- */

SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS customers,
    COUNT(DISTINCT t.transaction_id) AS transactions,
    SUM(t.revenue) AS total_revenue,
    ROUND(SUM(t.revenue) / COUNT(DISTINCT c.customer_id), 2) AS LTV,
    ROUND(SUM(t.revenue) / COUNT(DISTINCT t.transaction_id), 2) AS AOV,
    ROUND(COUNT(DISTINCT t.transaction_id) / COUNT(DISTINCT c.customer_id), 2) AS purchase_frequency
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

/* RESULTS:
   Segment     Customers  Transactions  Revenue      LTV      AOV     Frequency
   Premium     951        3,045         741,974.21   780.20   243.67  3.20
   Standard    914        2,928         720,108.35   787.86   245.94  3.20
   Basic       485        1,489         366,728.29   756.14   246.29  3.07
   Unassigned  457        1,430         351,102.51   768.28   245.53  3.13
   
   INTERPRETATION:
   
   SEGMENT COMPARISON:
   • Premium: Most customers (951), highest total revenue ($742K)
   • Standard: Second largest (914), slightly higher LTV ($787.86)
   • Basic: Smallest paying segment (485), lowest LTV ($756.14)
   • Unassigned: 457 customers not assigned to any segment
   
   KEY OBSERVATIONS:
   • LTV range is narrow: $756.14 to $787.86 (only 4.2% variance)
   • AOV is nearly identical across segments ($243-$246)
   • Purchase frequency is consistent (3.07 to 3.20)
   • Segments behave very similarly in purchasing patterns
   
   DATA QUALITY NOTE:
   The segments show minimal behavioral differentiation.
   This suggests the segmentation criteria may need review,
   or segments are based on attributes other than purchasing behavior.
*/


/* ================================================================================
   CAMPAIGN & SEGMENTATION SUMMARY
   ================================================================================
   
   TOP CAMPAIGNS BY REVENUE:
   1. Dec 2023 A - $24,151 (highest revenue)
   2. May 2023 A - $23,467
   3. Mar 2024 B - $21,246
   
   TOP CAMPAIGNS BY EFFICIENCY:
   1. Mar 2024 B - $31 per email delivered
   2. Mar 2023 A - $27 per email delivered
   3. Jan 2023 A - $25 per email delivered
   
   BOTTOM CAMPAIGNS BY REVENUE:
   1. Nov 2024 B - $12,879
   2. Dec 2023 B - $13,698
   3. Sep 2024 B - $14,028
   
   CAMPAIGN PATTERNS:
   • Variant A dominates top revenue spots (7 of top 10)
   • Variant B has higher open rates but lower total revenue
   • Mar 2024 B achieved best efficiency despite lower volume
   • Holiday campaigns show mixed results
   
   CUSTOMER SEGMENTS:
   • 4 segments: Premium, Standard, Basic, Unassigned
   • Minimal behavioral differentiation between segments
   • LTV variance is only 4.2% across all segments
   • AOV and frequency nearly identical
   
   KEY INSIGHT:
   Customer segments don't show meaningful differences in 
   purchasing behavior (LTV, AOV, frequency are nearly equal).
   Consider reviewing segmentation criteria.
   
   ================================================================================
*/
