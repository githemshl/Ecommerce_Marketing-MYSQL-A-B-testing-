/* ================================================================================
   02_AB_Test_Analysis.sql
   ================================================================================
   
   Purpose: Evaluate email subject line A/B test performance
   
   Business Question: Does the personalized subject line (Variant B: "Exclusive 
   Deal Just For You") outperform the standard subject line (Variant A: "Limited 
   Time Offer") in terms of engagement and revenue?
   
   Skills Demonstrated:
   • INNER JOIN for combining tables
   • CASE WHEN for conditional aggregation
   • CTEs to avoid row explosion
   • Business metric calculations
   
   ================================================================================
*/

USE ecommerce_marketing;

/* ----------------------------------------------------------------------------
   2.1 EMAIL ENGAGEMENT METRICS BY VARIANT
   ---------------------------------------------------------------------------- */

SELECT 
    c.variant,  
    COUNT(e.sent_status) AS delivered_cnt, 
    SUM(CASE WHEN e.opened = '1' THEN 1 ELSE 0 END) AS opened_cnt,
    SUM(CASE WHEN e.clicked = '1' THEN 1 ELSE 0 END) AS clicked_cnt,
    ROUND(SUM(CASE WHEN e.opened = '1' THEN 1 ELSE NULL END)
        / SUM(CASE WHEN e.sent_status = 'Delivered' THEN 1 ELSE NULL END) * 100, 2) AS open_rate,
    ROUND(SUM(e.clicked) / COUNT(*) * 100, 2) AS click_rate,
    ROUND(SUM(e.clicked) / SUM(e.opened) * 100, 2) AS CTOR
FROM email_campaigns c
INNER JOIN email_events e ON c.campaign_id = e.campaign_id
WHERE e.sent_status = 'Delivered'
GROUP BY c.variant;

/* RESULTS:
   Variant  Delivered  Opened  Clicked  Open_Rate  Click_Rate  CTOR
   A        24,914     2,256   651      9.06       2.61        28.86
   B        24,728     2,765   945      11.18      3.82        34.18
   
   INTERPRETATION:
   • Variant B has higher open rate: 11.18% vs 9.06% (+23.4% lift)
   • Variant B has higher click rate: 3.82% vs 2.61% (+46.4% lift)
   • Variant B has higher CTOR: 34.18% vs 28.86% (+18.4% lift)
   • Personalized subject line outperforms standard in all engagement metrics
   
   LIFT CALCULATIONS:
   Open Rate Lift: (11.18 - 9.06) / 9.06 * 100 = +23.4%
   Click Rate Lift: (3.82 - 2.61) / 2.61 * 100 = +46.4%
   CTOR Lift: (34.18 - 28.86) / 28.86 * 100 = +18.4%
*/


/* ----------------------------------------------------------------------------
   2.2 REVENUE METRICS BY VARIANT (Using CTEs to avoid row explosion)
   ---------------------------------------------------------------------------- */

WITH email_metrics AS (
    SELECT 
        c.variant, 
        COUNT(CASE WHEN e.sent_status = 'Delivered' THEN 1 ELSE NULL END) AS Delivered,
        COUNT(CASE WHEN e.clicked = '1' THEN 1 ELSE NULL END) AS Clicked 
    FROM email_campaigns c
    INNER JOIN email_events e ON c.campaign_id = e.campaign_id
    GROUP BY c.variant
),
revenue_metrics AS (
    SELECT 
        c.variant, 
        COUNT(t.transaction_id) AS Total_transactions, 
        SUM(t.revenue) AS Total_revenue
    FROM email_campaigns c
    INNER JOIN transactions t ON c.campaign_id = t.campaign_id
    GROUP BY c.variant
)
SELECT 
    e.variant, 
    r.Total_transactions, 
    r.Total_revenue, 
    ROUND(r.Total_revenue / e.Delivered, 2) AS Revenue_per_delivered,
    ROUND(r.Total_revenue / e.Clicked, 2) AS Revenue_per_click
FROM email_metrics e
INNER JOIN revenue_metrics r ON e.variant = r.variant;

/* RESULTS:
   Variant  Transactions  Revenue      Rev/Delivered  Rev/Click
   A        1,833         448,375.97   18.00          410.98
   B        1,681         412,569.72   16.68          254.52
   
   INTERPRETATION:
   • Variant A generates more transactions (1,833 vs 1,681)
   • Variant A generates more total revenue ($448,376 vs $412,570)
   • Variant A has higher revenue per delivered email ($18.00 vs $16.68)
   • Variant A has much higher revenue per click ($410.98 vs $254.52)
   • Variant A clickers are more purchase-ready than Variant B clickers
*/


/* ----------------------------------------------------------------------------
   2.3 CLICK-TO-PURCHASE RATE BY VARIANT
   ---------------------------------------------------------------------------- */

WITH email_metrics AS (
    SELECT 
        c.variant, 
        COUNT(CASE WHEN e.clicked = '1' THEN 1 ELSE NULL END) AS Total_clicks
    FROM email_campaigns c 
    INNER JOIN email_events e ON c.campaign_id = e.campaign_id
    GROUP BY c.variant
),
revenue_metrics AS (
    SELECT 
        c.variant, 
        COUNT(t.transaction_id) AS Total_transactions
    FROM email_campaigns c
    INNER JOIN transactions t ON c.campaign_id = t.campaign_id
    GROUP BY c.variant
)
SELECT 
    e.variant, 
    e.Total_clicks, 
    r.Total_transactions, 
    ROUND(r.Total_transactions / e.Total_clicks * 100, 2) AS Click_to_Purchase_Rate
FROM email_metrics e 
INNER JOIN revenue_metrics r ON e.variant = r.variant;

/* RESULTS:
   Variant  Clicks  Transactions  Click_to_Purchase_Rate
   A        1,091   1,833         168.01
   B        1,621   1,681         103.70
   
   INTERPRETATION:
   • Rate over 100% because one click can lead to multiple transactions
   • Variant A: 168% - each clicker generates 1.68 transactions on average
   • Variant B: 104% - each clicker generates 1.04 transactions on average
   • Variant A attracts higher-intent customers who make repeat purchases
   • Variant B attracts more browsers who may not convert as well
*/


/* ================================================================================
   A/B TEST SUMMARY
   ================================================================================
   
   ENGAGEMENT WINNER: Variant B
   • +23.4% higher open rate
   • +46.4% higher click rate
   • +18.4% higher click-to-open rate
   
   REVENUE WINNER: Variant A
   • $35,806 more total revenue
   • $18.00 vs $16.68 revenue per email delivered
   • $410.98 vs $254.52 revenue per click
   • 168% vs 104% click-to-purchase rate
   
   KEY INSIGHT:
   Variant B wins at getting attention and engagement.
   Variant A wins at converting engaged users to paying customers.
   
   The personalized subject line (B) attracts more opens and clicks,
   but those users are less likely to purchase compared to Variant A users.
   
   ================================================================================
*/
