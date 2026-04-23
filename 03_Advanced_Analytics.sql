-- Revenue trends with running totals and growth
CREATE OR REPLACE TABLE revenue_trends AS
SELECT
    order_date,
    region,
    daily_revenue,
    SUM(daily_revenue) OVER (
        PARTITION BY region 
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue,
    LAG(daily_revenue) OVER (
        PARTITION BY region 
        ORDER BY order_date
    ) as prev_day_revenue,
    ROUND(
        ((daily_revenue - LAG(daily_revenue) OVER (
            PARTITION BY region 
            ORDER BY order_date
        )) / LAG(daily_revenue) OVER (
            PARTITION BY region 
            ORDER BY order_date
        ) * 100), 2
    ) as revenue_growth_pct,
    AVG(daily_revenue) OVER (
        PARTITION BY region 
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day
FROM (
    SELECT
        order_date,
        region,
        ROUND(SUM(order_amount), 2) as daily_revenue
    FROM ecommerce_silver
    GROUP BY order_date, region
)
ORDER BY region, order_date DESC
LIMIT 100;

-- Display results
SELECT * FROM revenue_trends LIMIT 50;
-- Top 5 products by revenue in each region
CREATE OR REPLACE TABLE top_products_by_region AS
WITH ranked_products AS (
    SELECT
        region,
        product_name,
        category,
        ROUND(SUM(order_amount), 2) as total_revenue,
        ROUND(SUM(quantity), 0) as total_units,
        COUNT(*) as order_count,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(order_amount) DESC) as rank_in_region
    FROM ecommerce_silver
    GROUP BY region, product_name, category
)
SELECT * FROM ranked_products
WHERE rank_in_region <= 5
ORDER BY region, rank_in_region;

-- Display results
SELECT * FROM top_products_by_region;
-- Customer cohort analysis (retention by cohort month)
CREATE OR REPLACE TABLE customer_cohorts AS
WITH customer_cohorts_base AS (
    SELECT
        DATE_TRUNC('month', cm.first_purchase_date) as cohort_month,
        cm.customer_id,
        es.order_date,
        es.order_amount
    FROM customer_metrics cm
    JOIN ecommerce_silver es ON cm.customer_id = es.customer_id
),
cohort_with_month AS (
    SELECT
        cohort_month,
        DATE_TRUNC('month', order_date) as order_month,
        DATEDIFF(
            MONTH,
            cohort_month,
            DATE_TRUNC('month', order_date)
        ) as months_since_cohort,
        customer_id
    FROM customer_cohorts_base
)
SELECT
    cohort_month,
    months_since_cohort,
    COUNT(DISTINCT customer_id) as cohort_size,
    COUNT(*) as repeat_purchases,
    ROUND(COUNT(DISTINCT customer_id) / FIRST_VALUE(COUNT(DISTINCT customer_id)) 
        OVER (PARTITION BY cohort_month ORDER BY months_since_cohort) * 100, 2) as retention_rate_pct
FROM cohort_with_month
GROUP BY cohort_month, months_since_cohort
ORDER BY cohort_month DESC, months_since_cohort
LIMIT 50;

-- Display results
SELECT * FROM customer_cohorts;
-- Payment method performance
CREATE OR REPLACE TABLE payment_analysis AS
SELECT
    payment_method,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_users,
    ROUND(SUM(order_amount), 2) as total_revenue,
    ROUND(AVG(order_amount), 2) as avg_transaction_value,
    ROUND(SUM(CASE WHEN is_returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as return_rate_pct,
    ROUND(AVG(delivery_days), 1) as avg_delivery_days,
    ROUND(SUM(order_amount) / COUNT(DISTINCT customer_id), 2) as revenue_per_customer,
    ROUND(SUM(order_amount) * 100.0 / (SELECT SUM(order_amount) FROM ecommerce_silver), 2) as pct_of_total_revenue
FROM ecommerce_silver
GROUP BY payment_method
ORDER BY total_revenue DESC;

-- Display results
SELECT * FROM payment_analysis;
