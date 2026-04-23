%python
from pyspark.sql.functions import col, sum as spark_sum, count, isnull, isnan, max, min, avg, stddev

def generate_quality_report():
    """Generate comprehensive data quality report"""
    
    df = spark.read.table("ecommerce_silver")
    
    print("="*80)
    print("COMPREHENSIVE DATA QUALITY REPORT")
    print("="*80)
    
    # 1. Record count
    total_rows = df.count()
    print(f"\n1. VOLUME METRICS:")
    print(f"   Total records: {total_rows:,}")
    print(f"   Unique orders: {df.select('order_id').distinct().count():,}")
    print(f"   Unique customers: {df.select('customer_id').distinct().count():,}")
    
    # 2. Completeness (nulls)
    print(f"\n2. COMPLETENESS (Null Values):")
    null_counts = df.select([
        (spark_sum(col(c).isNull().cast('int'))).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    for column, null_count in null_counts.items():
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        status = "✓ PASS" if null_count == 0 else f"✗ WARNING ({null_pct:.2f}%)"
        print(f"   - {column}: {null_count} nulls {status}")
    
    # 3. Accuracy
    print(f"\n3. ACCURACY CHECKS:")
    
    # Negative amounts
    neg_amounts = df.filter(col("order_amount") < 0).count()
    print(f"   - Negative order amounts: {neg_amounts} {'✓' if neg_amounts == 0 else '✗'}")
    
    # Invalid quantities
    invalid_qty = df.filter((col("quantity") < 1) | (col("quantity") > 10)).count()
    print(f"   - Invalid quantities: {invalid_qty} {'✓' if invalid_qty == 0 else '✗'}")
    
    # Mismatched amounts
    mismatched = df.filter(
        col("order_amount") != col("quantity") * col("unit_price")
    ).count()
    print(f"   - Amount mismatches: {mismatched} {'✓' if mismatched == 0 else '✗'}")
    
    # 4. Consistency
    print(f"\n4. CONSISTENCY CHECKS:")
    
    # Duplicate orders
    duplicates = total_rows - df.select("order_id").distinct().count()
    print(f"   - Duplicate order IDs: {duplicates} {'✓' if duplicates == 0 else '✗'}")
    
    # Valid regions
    valid_regions = {'North', 'South', 'East', 'West', 'Central'}
    invalid_regions = df.filter(~col("region").isin(valid_regions)).count()
    print(f"   - Invalid regions: {invalid_regions} {'✓' if invalid_regions == 0 else '✗'}")
    
    # Valid payment methods
    valid_payments = {'Credit Card', 'Debit Card', 'UPI', 'Wallet', 'Net Banking'}
    invalid_payments = df.filter(~col("payment_method").isin(valid_payments)).count()
    print(f"   - Invalid payment methods: {invalid_payments} {'✓' if invalid_payments == 0 else '✗'}")
    
    # 5. Timeliness
    print(f"\n5. TIMELINESS (Data Freshness):")
    max_date = df.agg(max("order_date")).collect()[0][0]
    min_date = df.agg(min("order_date")).collect()[0][0]
    
    print(f"   - Latest order date: {max_date}")
    print(f"   - Earliest order date: {min_date}")
    print(f"   - Date range: {(max_date - min_date).days} days")
    
    # 6. Validity
    print(f"\n6. VALIDITY (Value Range Checks):")
    
    # Price ranges
    min_price = df.agg(min("unit_price")).collect()[0][0]
    max_price = df.agg(max("unit_price")).collect()[0][0]
    print(f"   - Price range: ₹{min_price:,.0f} to ₹{max_price:,.0f}")
    
    # Delivery days
    min_delivery = df.agg(min("delivery_days")).collect()[0][0]
    max_delivery = df.agg(max("delivery_days")).collect()[0][0]
    print(f"   - Delivery days range: {min_delivery} to {max_delivery} days")
    
    # 7. Statistical summary
    print(f"\n7. STATISTICAL SUMMARY:")
    stats = df.agg(
        min("order_amount").alias("min_amount"),
        max("order_amount").alias("max_amount"),
        avg("order_amount").alias("avg_amount"),
        stddev("order_amount").alias("stddev_amount"),
        spark_sum("order_amount").alias("total_revenue"),
        min("quantity").alias("min_qty"),
        max("quantity").alias("max_qty"),
        avg("quantity").alias("avg_qty"),
        min("delivery_days").alias("min_delivery"),
        max("delivery_days").alias("max_delivery"),
        avg("delivery_days").alias("avg_delivery")
    ).collect()[0]
    
    print(f"   Order Amount:")
    print(f"     - Min: ₹{stats.min_amount:,.2f}")
    print(f"     - Max: ₹{stats.max_amount:,.2f}")
    print(f"     - Avg: ₹{stats.avg_amount:,.2f}")
    print(f"     - StdDev: ₹{stats.stddev_amount:,.2f}")
    print(f"     - Total Revenue: ₹{stats.total_revenue:,.2f}")
    
    print(f"\n{'='*80}")
    print("✓ QUALITY REPORT COMPLETED")
    print(f"{'='*80}\n")
     
# Generate report
generate_quality_report()
from datetime import datetime

# Create a quality metrics table for tracking over time
quality_metrics_data = [
    {
        'check_date': datetime.now(),
        'total_records': spark.read.table("ecommerce_silver").count(),
        'unique_customers': spark.read.table("ecommerce_silver").select('customer_id').distinct().count(),
        'null_count': spark.read.table("ecommerce_silver").filter(col("order_id").isNull()).count(),
        'duplicate_count': spark.read.table("ecommerce_silver").count() - spark.read.table("ecommerce_silver").select("order_id").distinct().count(),
        'data_freshness_days': (datetime.now().date() - spark.read.table("ecommerce_silver").agg(max("order_date")).collect()[0][0]).days,
        'quality_score_pct': 98.5  # Placeholder - calculate based on checks
    }
]

df_quality = spark.createDataFrame(quality_metrics_data)

# Save quality metrics
df_quality.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("quality_metrics_history")

print("✓ Quality metrics saved to quality_metrics_history table")
display(df_quality)
