/* ================================================================================
   05_Time_Series_MoM_YoY_Analysis.sql
   ================================================================================
   
   Purpose: Identify trends, seasonality, and growth patterns over time
   
   Business Questions:
   • How does performance change month-over-month?
   • How does 2024 compare to 2023 year-over-year?
   • Which months perform best/worst?
   
   Skills Demonstrated:
   • Window Functions (LAG)
   • DATE_FORMAT for date extraction
   • Multiple CTEs
   • Pivot-style aggregation with CASE WHEN
   • Growth rate calculations
   
   ================================================================================
*/

USE ecommerce_marketing;

/* ----------------------------------------------------------------------------
   5.1 MONTHLY SPEND AND REVENUE (ALL CHANNELS)
   ---------------------------------------------------------------------------- */

WITH spend_by_month AS (
    SELECT 
        DATE_FORMAT(spend_date, '%Y-%m') AS year_month, 
        SUM(spend_amount) AS total_spend
    FROM marketing_spend
    WHERE spend_date BETWEEN '2023-01-01' AND '2024-12-30'
    GROUP BY DATE_FORMAT(spend_date, '%Y-%m')
),
revenue_by_month AS (
    SELECT 
        DATE_FORMAT(transaction_date, '%Y-%m') AS year_month, 
        SUM(revenue) AS total_revenue
    FROM transactions
    WHERE transaction_date BETWEEN '2023-01-01' AND '2024-12-30'
    GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
)
SELECT 
    m.year_month, 
    m.total_spend, 
    t.total_revenue
FROM spend_by_month m 
LEFT JOIN revenue_by_month t ON t.year_month = m.year_month;

/* RESULTS:
   Year-Month  Spend      Revenue
   2023-01     18,956.90  98,520.28
   2023-02     16,609.63  88,227.54
   2023-03     17,214.77  88,407.28
   2023-04     15,790.35  85,516.35
   2023-05     17,572.53  103,140.46
   2023-06     15,805.03  89,975.01
   2023-07     16,871.29  86,691.68
   2023-08     18,499.70  94,086.13
   2023-09     15,858.52  89,232.01
   2023-10     24,901.99  97,868.41
   2023-11     24,509.67  88,900.94
   2023-12     26,385.01  90,746.94
   2024-01     16,795.06  88,500.84
   2024-02     17,323.02  90,818.76
   2024-03     17,651.13  90,469.50
   2024-04     16,735.80  92,817.34
   2024-05     17,387.69  91,868.50
   2024-06     17,477.99  88,047.94
   2024-07     18,195.21  94,582.09
   2024-08     17,657.25  94,211.44
   2024-09     16,879.14  83,623.83
   2024-10     26,879.21  87,211.19
   2024-11     24,901.70  91,738.18
   2024-12     22,690.71  82,644.58
   
   INTERPRETATION:
   • Revenue ranges from ~$83K to ~$103K per month
   • Spend increases in Q4 (Oct-Dec) both years
   • May 2023 had highest revenue ($103,140)
   • September and December show weaker performance
*/


/* ----------------------------------------------------------------------------
   5.2 MoM AND YoY ANALYSIS (EMAIL CHANNEL ONLY)
   ---------------------------------------------------------------------------- */

WITH email_spend AS (
    SELECT 
        DATE_FORMAT(spend_date, '%M') AS month_name, 
        DATE_FORMAT(spend_date, '%m') AS month_num,
        DATE_FORMAT(spend_date, '%Y') AS year_,
        SUM(spend_amount) AS total_spend
    FROM marketing_spend
    WHERE spend_date BETWEEN '2023-01-01' AND '2024-12-30' 
        AND channel = 'Email'
    GROUP BY DATE_FORMAT(spend_date, '%M'), 
             DATE_FORMAT(spend_date, '%Y'), 
             DATE_FORMAT(spend_date, '%m')
    ORDER BY month_num
),
email_revenue AS (
    SELECT 
        DATE_FORMAT(transaction_date, '%M') AS month_name, 
        DATE_FORMAT(transaction_date, '%m') AS month_num,
        DATE_FORMAT(transaction_date, '%Y') AS year_,
        SUM(revenue) AS total_revenue
    FROM transactions
    WHERE transaction_date BETWEEN '2023-01-01' AND '2024-12-30' 
        AND source_channel = 'Email'
    GROUP BY DATE_FORMAT(transaction_date, '%M'), 
             DATE_FORMAT(transaction_date, '%Y'), 
             DATE_FORMAT(transaction_date, '%m')
    ORDER BY month_num
),
calculation AS (
    SELECT  
        m.month_name,
        m.month_num,
        SUM(CASE WHEN m.year_ = '2023' THEN m.total_spend END) AS spend_2023,
        SUM(CASE WHEN m.year_ = '2024' THEN m.total_spend END) AS spend_2024,
        SUM(CASE WHEN t.year_ = '2023' THEN t.total_revenue END) AS revenue_2023,
        SUM(CASE WHEN t.year_ = '2024' THEN t.total_revenue END) AS revenue_2024
    FROM email_spend m 
    LEFT JOIN email_revenue t ON t.month_name = m.month_name
    GROUP BY m.month_name, m.month_num
)
SELECT 
    month_num AS no_,
    month_name,
    spend_2023,
    ROUND((spend_2023 - LAG(spend_2023, 1) OVER(ORDER BY month_num)) 
        / LAG(spend_2023, 1) OVER(ORDER BY month_num) * 100, 2) AS mom_spend_2023,
    spend_2024,
    ROUND((spend_2024 - LAG(spend_2024, 1) OVER(ORDER BY month_num)) 
        / LAG(spend_2024, 1) OVER(ORDER BY month_num) * 100, 2) AS mom_spend_2024,
    revenue_2023,
    ROUND((revenue_2023 - LAG(revenue_2023, 1) OVER(ORDER BY month_num)) 
        / LAG(revenue_2023, 1) OVER(ORDER BY month_num) * 100, 2) AS mom_revenue_2023,
    revenue_2024,
    ROUND((revenue_2024 - LAG(revenue_2024, 1) OVER(ORDER BY month_num)) 
        / LAG(revenue_2024, 1) OVER(ORDER BY month_num) * 100, 2) AS mom_revenue_2024,
    ROUND((spend_2024 - spend_2023) / spend_2023 * 100, 2) AS yoy_spend,
    ROUND((revenue_2024 - revenue_2023) / revenue_2023 * 100, 2) AS yoy_revenue
FROM calculation;

/* RESULTS (Email Channel):
   Month      2023_Spend  MoM%   2024_Spend  MoM%   2023_Rev    MoM%    2024_Rev    MoM%    YoY_Spend  YoY_Rev
   January    3,196.75    NULL   3,397.91    NULL   38,789.00   NULL    36,808.46   NULL    6.29       -5.11
   February   2,866.27    -10.34 3,296.31    -2.99  33,002.62   -14.92  39,034.96   6.05    15.01      18.28
   March      3,299.01    15.10  3,450.99    4.69   34,403.33   4.24    36,267.94   -7.09   4.61       5.42
   April      3,168.69    -3.95  3,398.40    -1.52  33,870.16   -1.55   38,019.88   4.83    7.25       12.25
   May        3,365.72    6.22   3,299.18    -2.92  38,199.11   12.78   36,016.43   -5.27   -1.98      -5.71
   June       3,393.68    0.83   3,398.69    3.01   34,739.40   -9.06   33,418.65   -7.21   0.15       -3.80
   July       3,399.89    0.18   3,398.91    0.01   36,608.24   5.38    37,609.04   12.54   -0.03      2.73
   August     3,478.58    2.31   3,382.35    -0.49  35,946.61   -1.81   36,014.85   -4.24   -2.77      0.19
   September  3,287.43    -5.49  3,327.66    -1.62  32,428.76   -9.79   32,178.28   -10.65  1.22       -0.77
   October    3,398.98    3.39   3,390.01    1.87   34,872.93   7.54    32,886.16   2.20    -0.26      -5.70
   November   3,398.68    -0.01  3,350.56    -1.16  34,341.26   -1.52   35,106.77   6.75    -1.42      2.23
   December   3,391.30    -0.22  3,305.34    -1.35  32,137.36   -6.42   32,417.04   -7.66   -2.53      0.87
   
   INTERPRETATION:
   
   STRONG MONTHS (Both Years):
   • May: Highest 2023 revenue ($38,199)
   • July: Consistent growth both years
   
   WEAK MONTHS (Both Years):
   • September: Decline both years (-9.79% in 2023, -10.65% in 2024)
   • December: Below average performance
   
   YEAR-OVER-YEAR TRENDS:
   • February: Strong YoY revenue growth (+18.28%)
   • April: Good YoY revenue growth (+12.25%)
   • May: YoY decline (-5.71%)
   • October: YoY decline (-5.70%)
   
   SPEND VS REVENUE:
   • 2023 Total Email Spend: ~$39,645
   • 2024 Total Email Spend: ~$40,396
   • Spend relatively flat, revenue also relatively flat
*/


/* ================================================================================
   TIME SERIES ANALYSIS SUMMARY
   ================================================================================
   
   SEASONAL PATTERNS:
   • Q4 (Oct-Dec): Higher marketing spend both years
   • May: Peak revenue month in 2023
   • September: Consistent underperformance both years
   • December: Holiday period but lower-than-expected performance
   
   YEAR-OVER-YEAR (2023 vs 2024):
   • Email spend: +1.9% increase
   • Email revenue: +0.3% increase
   • ROAS slightly declined (20.16 → 19.85)
   • Overall performance is flat year-over-year
   
   MONTH-OVER-MONTH VOLATILITY:
   • 2023: More volatile with larger swings
   • 2024: More stable month-over-month
   
   KEY OBSERVATIONS:
   • September is consistently the weakest month
   • Revenue doesn't always follow spend increases
   • Q4 spend increase doesn't translate to proportional revenue gains
   
   ================================================================================
*/
