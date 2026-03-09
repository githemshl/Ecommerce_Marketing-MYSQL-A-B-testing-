/* ================================================================================
   04_Funnel_Analysis.sql
   ================================================================================
   
   Purpose: Identify drop-off points in the email marketing customer journey
   
   Business Question: At which stage do we lose the most potential customers 
   in the email journey?
   
   Skills Demonstrated:
   • Multiple CTEs
   • Funnel stage calculations
   • Conversion rate analysis
   • JOIN for combining metrics
   
   ================================================================================
*/

USE ecommerce_marketing;

/* ----------------------------------------------------------------------------
   4.1 EMAIL MARKETING FUNNEL BY VARIANT
   Stages: Sent → Delivered → Opened → Clicked → Purchased
   ---------------------------------------------------------------------------- */

WITH purchase_metrics AS (
    SELECT
        c.variant,
        SUM(CASE WHEN t.order_status = 'Completed' 
                  AND t.source_channel = 'Email' THEN 1 ELSE 0 END) AS purchases
    FROM email_campaigns c
    JOIN transactions t ON c.campaign_id = t.campaign_id
    GROUP BY c.variant
),
engagement_metrics AS (
    SELECT
        c.variant,
        COUNT(*) AS total_sent,
        SUM(CASE WHEN e.sent_status = 'Delivered' THEN 1 ELSE 0 END) AS delivered,
        SUM(CASE WHEN e.opened = 1 THEN 1 ELSE 0 END) AS opened,
        SUM(CASE WHEN e.clicked = 1 THEN 1 ELSE 0 END) AS clicked
    FROM email_events e
    JOIN email_campaigns c ON c.campaign_id = e.campaign_id
    GROUP BY c.variant
)
SELECT 
    e.variant,
    e.total_sent,
    e.delivered,
    ROUND(e.delivered / e.total_sent * 100, 2) AS sent_to_delivered,
    e.opened,
    ROUND(e.opened / e.delivered * 100, 2) AS delivered_to_opened,
    e.clicked,
    ROUND(e.clicked / e.opened * 100, 2) AS opened_to_clicked,
    p.purchases,
    ROUND(p.purchases / e.clicked * 100, 2) AS clicked_to_purchased,
    ROUND(p.purchases / e.total_sent * 100, 2) AS overall_conversion
FROM engagement_metrics e
JOIN purchase_metrics p ON e.variant = p.variant;

/* RESULTS:
   Variant  Sent    Delivered  Del%    Opened  Open%   Clicked  Click%  Purchases  Purch%  Overall%
   A        42,715  24,914     58.33   3,795   15.23   1,091    28.75   345        31.62   0.81
   B        42,508  24,728     58.17   4,851   19.62   1,621    33.42   307        18.94   0.72
   
   FUNNEL BREAKDOWN:
   
   VARIANT A:
   • Sent → Delivered: 58.33% (41.67% lost to bounce/fail)
   • Delivered → Opened: 15.23% (84.77% didn't open)
   • Opened → Clicked: 28.75% (71.25% didn't click)
   • Clicked → Purchased: 31.62% (68.38% didn't buy)
   • Overall: 0.81% conversion
   
   VARIANT B:
   • Sent → Delivered: 58.17% (41.83% lost to bounce/fail)
   • Delivered → Opened: 19.62% (80.38% didn't open)
   • Opened → Clicked: 33.42% (66.58% didn't click)
   • Clicked → Purchased: 18.94% (81.06% didn't buy)
   • Overall: 0.72% conversion
   
   INTERPRETATION:
   • Biggest drop-off: Delivered → Opened (~80-85% loss)
   • Variant B outperforms A at every engagement stage
   • BUT Variant A has higher click-to-purchase rate (31.62% vs 18.94%)
   • Variant A has higher overall conversion (0.81% vs 0.72%)
   • Variant B attracts more opens/clicks but less qualified buyers
*/


/* ================================================================================
   FUNNEL ANALYSIS SUMMARY
   ================================================================================
   
   DROP-OFF POINTS (Both Variants):
   1. Sent → Delivered: ~42% loss (bounce/delivery failures)
   2. Delivered → Opened: ~80-85% loss (biggest drop-off)
   3. Opened → Clicked: ~67-71% loss
   4. Clicked → Purchased: ~69-81% loss
   
   VARIANT COMPARISON:
   
   Stage               Variant A    Variant B    Winner
   ------------------- ------------ ------------ --------
   Delivery Rate       58.33%       58.17%       A (tie)
   Open Rate           15.23%       19.62%       B
   Click Rate (CTOR)   28.75%       33.42%       B
   Purchase Rate       31.62%       18.94%       A
   Overall Conversion  0.81%        0.72%        A
   
   KEY INSIGHT:
   Variant B wins at engagement (opens, clicks).
   Variant A wins at conversion (purchases).
   
   The personalized subject line (B) gets more attention,
   but the standard subject line (A) attracts buyers with higher intent.
   
   OPPORTUNITY:
   The largest opportunity is improving the Delivered → Opened stage,
   where 80-85% of recipients don't engage with the email.
   
   ================================================================================
*/
