import pandas as pd
import random
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.types import *


print("All imports done")
print(f"✓ Spark version: {spark.version}")

def generate_ecommerce_data(num_records=50000):
    """
    Generate realistic e-commerce transaction data
    
    Args:
        num_records: Number of transactions to generate (default: 50,000)
    
    Returns:
        List of dictionaries containing transaction data
    """
    
    # Data pools
    products = [
        'Laptop', 'Desktop', 'Monitor', 'Keyboard', 'Mouse',
        'Headphones', 'Speaker', 'Webcam', 'Phone', 'Tablet',
        'Charger', 'Cable', 'Dock', 'Stand', 'Mousepad'
    ]
    
    categories = [
        'Computers', 'Peripherals', 'Mobile', 'Accessories'
    ]
    
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    payment_methods = [
        'Credit Card', 'Debit Card', 'UPI', 'Wallet', 'Net Banking'
    ]
    
    # Generate data
    data = []
    
    for i in range(num_records):
        # Random order date in last 90 days
        order_date = datetime.now() - timedelta(days=random.randint(0, 90))
        
        # Random product
        product = random.choice(products)
        
        # Get category based on product
        if product in ['Laptop', 'Desktop', 'Monitor']:
            category = 'Computers'
        elif product in ['Phone', 'Tablet']:
            category = 'Mobile'
        elif product in ['Keyboard', 'Mouse', 'Headphones', 'Speaker', 'Webcam']:
            category = 'Peripherals'
        else:
            category = 'Accessories'
        
        # Generate pricing based on category
        if category == 'Computers':
            price = __builtins__.round(random.uniform(15000, 150000), 2)
        elif category == 'Mobile':
            price = __builtins__.round(random.uniform(10000, 80000), 2)
        elif category == 'Peripherals':
            price = __builtins__.round(random.uniform(1000, 10000), 2)
        else:
            price = __builtins__.round(random.uniform(100, 5000), 2)
        
        # Create record
        data.append({
            'order_id': f'ORD_{i:07d}',
            'customer_id': f'CUST_{random.randint(1, 5000):05d}',
            'product_name': product,
            'category': category,
            'quantity': random.randint(1, 5),
            'unit_price': price,
            'order_date': order_date,
            'region': random.choice(regions),
            'payment_method': random.choice(payment_methods),
            'delivery_days': random.randint(1, 30),
            'is_returned': random.choice([0, 0, 0, 1])  # 25% return rate
        })
    
    return data

# Generate data
print("Generating sample e-commerce data...")
raw_data = generate_ecommerce_data(50000)
print(f"✓ Generated {len(raw_data):,} records")

# Show first 5 records
print("\nSample data:")
for record in raw_data[:5]:
    print(record)
    # Create Spark DataFrame from raw data
print("Creating Spark DataFrame...")
df_raw = spark.createDataFrame(raw_data)

# Show basic info
print(f"\n✓ Total records: {df_raw.count():,}")
print(f"✓ Columns: {df_raw.columns}")
print(f"\n✓ Schema:")
df_raw.printSchema()

# Display first 10 rows
print("\n✓ First 100  rows:")
display(df_raw.limit(100))
# Data quality checks
print("="*70)
print("DATA QUALITY CHECKS - RAW LAYER")
print("="*70)

# Check 1: Record count
total_records = df_raw.count()
print(f"\n1. TOTAL RECORDS: {total_records:,}")

# Check 2: Unique values
print(f"\n2. UNIQUE VALUES:")
print(f"   - Unique orders: {df_raw.select('order_id').distinct().count():,}")
print(f"   - Unique customers: {df_raw.select('customer_id').distinct().count():,}")
print(f"   - Unique products: {df_raw.select('product_name').distinct().count()}")
print(f"   - Unique categories: {df_raw.select('category').distinct().count()}")
print(f"   - Regions: {df_raw.select('region').distinct().count()}")

# Check 3: Null values
print(f"\n3. NULL VALUES:")
null_counts = df_raw.select([
    (col(c).isNull().cast('int')).alias(c) 
    for c in df_raw.columns
]).collect()[0].asDict()

for column, null_count in null_counts.items():
    status = "✓ PASS" if null_count == 0 else f"✗ FAIL ({null_count})"
    print(f"   - {column}: {status}")

# Check 4: Data types
print(f"\n4. DATA TYPES:")
for field in df_raw.schema.fields:
    print(f"   - {field.name}: {field.dataType}")

# Check 5: Price range
print(f"\n5. PRICE STATISTICS:")
price_stats = df_raw.agg({
    'unit_price': 'min',
    'unit_price': 'max'
}).collect()[0]

print(f"   - Min price: ₹{df_raw.agg({'unit_price': 'min'}).collect()[0][0]:,.2f}")
print(f"   - Max price: ₹{df_raw.agg({'unit_price': 'max'}).collect()[0][0]:,.2f}")
print(f"   - Avg price: ₹{df_raw.agg({'unit_price': 'avg'}).collect()[0][0]:,.2f}")

# Check 6: Data date range
print(f"\n6. DATE RANGE:")
min_date = df_raw.agg({'order_date': 'min'}).collect()[0][0]
max_date = df_raw.agg({'order_date': 'max'}).collect()[0][0]
print(f"   - Earliest order: {min_date}")
print(f"   - Latest order: {max_date}")

print(f"\n{'='*70}")
print("✓ ALL QUALITY CHECKS COMPLETED")
print(f"{'='*70}")
# Save raw data to Delta Lake (Bronze layer)
print("Saving data to Delta Lake...")

try:
    df_raw.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("ecommerce_bronze")
    
    print("✓ Successfully saved to ecommerce_bronze table")
    
    # Verify
    verify_df = spark.read.table("ecommerce_bronze")
    print(f"✓ Verified: {verify_df.count():,} records in Delta Lake")
    
    # Show table location
    print(f"\n✓ Table location: /user/hive/warehouse/ecommerce_bronze")
    print(f"✓ Format: Delta Lake (ACID compliant)")
    print(f"✓ Mode: Overwrite (latest data only)")
    
except Exception as e:
    print(f"✗ Error saving to Delta Lake: {e}")
    
