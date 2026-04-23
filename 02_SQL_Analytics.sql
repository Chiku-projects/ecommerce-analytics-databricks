-- Create daily sales summary
CREATE OR REPLACE TABLE daily_sales_summary AS
SELECT
    order_date,
    region,
    category,
    COUNT(*) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(order_amount), 2) as total_revenue,
    ROUND(AVG(order_amount), 2) as avg_order_value,
    ROUND(MIN(order_amount), 2) as min_order_value,
    ROUND(MAX(order_amount), 2) as max_order_value,
    ROUND(SUM(quantity), 0) as total_units_sold,
    ROUND(SUM(CASE WHEN is_returned = 1 THEN order_amount ELSE 0 END), 2) as returned_amount,
    ROUND(COUNT(CASE WHEN is_returned = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as return_rate_pct
FROM ecommerce_silver
GROUP BY order_date, region, category
ORDER BY order_date DESC, region;

-- Display results
SELECT * FROM daily_sales_summary LIMIT 20;

-- Customer Recency, Frequency, Monetary analysis
CREATE OR REPLACE TABLE customer_metrics AS
SELECT
    customer_id,
    COUNT(*) as total_purchases,
    MAX(order_date) as last_purchase_date,
    DATEDIFF(CURRENT_DATE, MAX(order_date)) as days_since_purchase,
    ROUND(SUM(order_amount), 2) as lifetime_value,
    ROUND(AVG(order_amount), 2) as avg_order_value,
    ROUND(STDDEV(order_amount), 2) as order_value_stddev,
    ROUND(SUM(quantity), 0) as total_items,
    MIN(order_date) as first_purchase_date,
    DATEDIFF(MAX(order_date), MIN(order_date)) as customer_tenure_days,
    ROUND(SUM(CASE WHEN is_returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as return_rate_pct
FROM ecommerce_silver
GROUP BY customer_id;

-- Display results
SELECT * FROM customer_metrics ORDER BY lifetime_value DESC;
-- Product performance analysis
CREATE OR REPLACE TABLE product_performance AS
SELECT
    product_name,
    category,
    COUNT(*) as times_purchased,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(quantity), 0) as total_units_sold,
    ROUND(SUM(order_amount), 2) as total_revenue,
    ROUND(AVG(order_amount), 2) as avg_order_value,
    ROUND(AVG(quantity), 2) as avg_units_per_order,
    ROUND(MIN(order_amount), 2) as min_order_value,
    ROUND(MAX(order_amount), 2) as max_order_value,
    ROUND(SUM(CASE WHEN is_returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as return_rate_pct,
    ROUND(SUM(order_amount) / SUM(quantity), 2) as revenue_per_unit,
    ROW_NUMBER() OVER (ORDER BY SUM(order_amount) DESC) as revenue_rank
FROM ecommerce_silver
GROUP BY product_name, category
ORDER BY total_revenue DESC;

-- Display results
SELECT * FROM product_performance;
-- Regional performance metrics
CREATE OR REPLACE TABLE regional_analysis AS
SELECT
    region,
    COUNT(*) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(order_amount), 2) as total_revenue,
    ROUND(AVG(order_amount), 2) as avg_order_value,
    ROUND(AVG(delivery_days), 1) as avg_delivery_days,
    ROUND(SUM(order_amount) / COUNT(DISTINCT customer_id), 2) as revenue_per_customer,
    ROUND(SUM(CASE WHEN is_returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as return_rate_pct,
    ROUND(SUM(quantity), 0) as total_units_sold
FROM ecommerce_silver
GROUP BY region
ORDER BY total_revenue DESC;

-- Display results
SELECT * FROM regional_analysis;
-- Category performance trends
CREATE OR REPLACE TABLE category_trends AS
SELECT
    order_month,
    category,
    COUNT(*) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(order_amount), 2) as monthly_revenue,
    ROUND(AVG(order_amount), 2) as avg_order_value,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY order_month DESC) as month_rank
FROM ecommerce_silver
GROUP BY order_month, category
ORDER BY order_month DESC, category;

-- Display results
SELECT * FROM category_trends LIMIT 30;
-- RFM Segmentation (combining Recency, Frequency, Monetary)
CREATE OR REPLACE TABLE customer_segmentation AS
WITH rfm_scores AS (
    SELECT
        customer_id,
        total_purchases,
        days_since_purchase,
        lifetime_value,
        -- Recency Score (lower days = higher score)
        NTILE(5) OVER (ORDER BY days_since_purchase DESC) as recency_score,
        -- Frequency Score (more purchases = higher score)
        NTILE(5) OVER (ORDER BY total_purchases ASC) as frequency_score,
        -- Monetary Score (higher value = higher score)
        NTILE(5) OVER (ORDER BY lifetime_value ASC) as monetary_score
    FROM customer_metrics
)
SELECT
    customer_id,
    recency_score,
    frequency_score,
    monetary_score,
    CONCAT(recency_score, frequency_score, monetary_score) as rfm_segment,
    CASE
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'VIP'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'High Value'
        WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal'
        WHEN recency_score <= 2 THEN 'At Risk'
        WHEN monetary_score >= 4 THEN 'Big Spender'
        ELSE 'Standard'
END as customer_segment,
    total_purchases,
    days_since_purchase,
    lifetime_value
FROM rfm_scores
ORDER BY lifetime_value DESC;

-- Display results
SELECT * FROM customer_segmentation LIMIT 30;

-- Delivery performance analysis
CREATE OR REPLACE TABLE delivery_performance AS
SELECT
    region,
    delivery_type,
    COUNT(*) as order_count,
    ROUND(AVG(delivery_days), 1) as avg_delivery_days,
    ROUND(MIN(delivery_days), 0) as min_delivery_days,
    ROUND(MAX(delivery_days), 0) as max_delivery_days,
    ROUND(PERCENTILE(delivery_days, 0.5), 1) as median_delivery_days,
    ROUND(PERCENTILE(delivery_days, 0.75), 1) as p75_delivery_days,
    ROUND(PERCENTILE(delivery_days, 0.95), 1) as p95_delivery_days,
    ROUND(SUM(order_amount), 2) as total_revenue
FROM ecommerce_silver
GROUP BY region, delivery_type
ORDER BY region, avg_delivery_days;

-- Display results
SELECT * FROM delivery_performance;
