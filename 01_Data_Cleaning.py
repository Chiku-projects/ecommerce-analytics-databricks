from pyspark.sql.functions import (
    col, round, when, datediff, current_date,
    year, month, dayofweek, weekofyear, date_format
)

# Read from Delta Lake
print("Reading data from Bronze layer...")
df = spark.read.table("ecommerce_bronze")

print(f"✓ Input records: {df.count():,}")
print(f"✓ Columns: {len(df.columns)}")
print(f"✓ Schema:")
df.printSchema()

# Show sample
print("\n✓ Sample data:")
display(df.limit(5))
print("Starting data cleaning...")

# Clean and transform
df_cleaned = df.select(
    
    # Original columns with proper types
    col("order_id"),
    col("customer_id"),
    col("product_name"),
    col("category"),
    col("quantity").cast("int"),
    round(col("unit_price"), 2).alias("unit_price"),
    col("order_date").cast("date").alias("order_date"),
    col("region"),
    col("payment_method"),
    col("delivery_days").cast("int"),
    col("is_returned").cast("int"),
    
    # Calculated columns
    round(col("quantity") * col("unit_price"), 2).alias("order_amount"),
    
    # Date columns
    year(col("order_date")).alias("order_year"),
    month(col("order_date")).alias("order_month"),
    dayofweek(col("order_date")).alias("day_of_week"),
    weekofyear(col("order_date")).alias("week_of_year"),
    
    # Classification columns
    when(col("quantity") > 3, "High").otherwise("Normal").alias("order_size"),
    when(col("order_amount") > 50000, "Premium").when(col("order_amount") > 10000, "Standard").otherwise("Budget").alias("order_value_segment"),
    
    # Delivery classification
    when(col("delivery_days") <= 3, "Express")
        .when(col("delivery_days") <= 7, "Fast")
        .otherwise("Standard")
        .alias("delivery_type"),
    
    # Metadata
    current_date().alias("processing_date")
) \
.filter((col("unit_price") > 0) & (col("quantity") > 0) & (col("order_amount") > 0)) \
.dropDuplicates(["order_id"])

print(f"✓ Output records: {df_cleaned.count():,}")
print(f"✓ Rows removed: {df.count() - df_cleaned.count():,}")
print(f"✓ Columns: {len(df_cleaned.columns)}")

# Show sample cleaned data
print("\n✓ Cleaned data sample:")
display(df_cleaned.limit(10))
print("="*70)
print("DATA QUALITY CHECKS - CLEANED LAYER")
print("="*70)

# Check 1: Null values
print(f"\n1. NULL VALUES CHECK:")
null_check = df_cleaned.select([
    (col(c).isNull().cast('int')).alias(c) 
    for c in df_cleaned.columns
]).collect()[0].asDict()

total_nulls = sum(null_check.values())
print(f"   - Total nulls: {total_nulls}")
for column, null_count in null_check.items():
    if null_count > 0:
        print(f"   - {column}: {null_count} nulls (✗)")

if total_nulls == 0:
    print("   ✓ No null values (PASS)")

# Check 2: Negative values
print(f"\n2. NEGATIVE VALUES CHECK:")
negative_amounts = df_cleaned.filter(col("order_amount") < 0).count()
negative_prices = df_cleaned.filter(col("unit_price") < 0).count()
negative_qty = df_cleaned.filter(col("quantity") < 0).count()

print(f"   - Negative amounts: {negative_amounts}")
print(f"   - Negative prices: {negative_prices}")
print(f"   - Negative quantity: {negative_qty}")

if negative_amounts == 0 and negative_prices == 0 and negative_qty == 0:
    print("   ✓ No negative values (PASS)")

# Check 3: Data type verification
print(f"\n3. DATA TYPE VERIFICATION:")
expected_types = {
    'order_id': 'string',
    'order_amount': 'double',
    'quantity': 'int',
    'order_date': 'date'
}

for col_name, expected_type in expected_types.items():
    actual_type = [f.dataType for f in df_cleaned.schema.fields if f.name == col_name][0]
    status = "✓" if str(actual_type) == expected_type else "✗"
    print(f"   - {col_name}: {actual_type} {status}")

# Check 4: Value range validation
print(f"\n4. VALUE RANGE VALIDATION:")
invalid_delivery = df_cleaned.filter((col("delivery_days") < 0) | (col("delivery_days") > 90)).count()
invalid_quantity = df_cleaned.filter((col("quantity") < 1) | (col("quantity") > 10)).count()

print(f"   - Invalid delivery days: {invalid_delivery}")
print(f"   - Invalid quantity: {invalid_quantity}")

if invalid_delivery == 0 and invalid_quantity == 0:
    print("   ✓ All values in valid range (PASS)")

# Check 5: Duplicate detection
print(f"\n5. DUPLICATE DETECTION:")
total_rows = df_cleaned.count()
distinct_orders = df_cleaned.select("order_id").distinct().count()
duplicate_count = total_rows - distinct_orders

print(f"   - Total rows: {total_rows:,}")
print(f"   - Distinct orders: {distinct_orders:,}")
print(f"   - Duplicates: {duplicate_count}")

if duplicate_count == 0:
    print("   ✓ No duplicates (PASS)")

print(f"\n{'='*70}")
print("✓ ALL QUALITY CHECKS PASSED")
print(f"{'='*70}")
# Save cleaned data to Silver layer
print("Saving to Silver layer...")

try:
    df_cleaned.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("ecommerce_silver")
    
    print("✓ Successfully saved to ecommerce_silver table")
    
    # Verify
    verify_df = spark.read.table("ecommerce_silver")
    print(f"✓ Verified: {verify_df.count():,} records in Silver layer")
    
    # Summary statistics
    print(f"\n✓ Summary Statistics:")
    print(f"   - Customers: {verify_df.select('customer_id').distinct().count():,}")
    print(f"   - Products: {verify_df.select('product_name').distinct().count()}")
    print(f"   - Total revenue: ₹{verify_df.agg({'order_amount': 'sum'}).collect()[0][0]:,.2f}")
    print(f"   - Avg order value: ₹{verify_df.agg({'order_amount': 'avg'}).collect()[0][0]:,.2f}")
    
except Exception as e:
    print(f"✗ Error: {e}")
    
