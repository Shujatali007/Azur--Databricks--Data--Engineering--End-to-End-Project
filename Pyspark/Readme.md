# 🔥 PySpark Fundamentals & Advanced Concepts

This folder contains comprehensive PySpark tutorials covering fundamental to advanced data processing techniques. These notebooks serve as both learning resources and reference implementations for data engineering tasks.

## 📋 Overview

PySpark is the Python API for Apache Spark, enabling large-scale data processing with Python. This collection demonstrates:
- DataFrame operations and transformations
- SQL integration and optimization
- Data manipulation techniques
- Performance tuning strategies
- Real-world data engineering patterns

---

## 📂 Files in This Folder

### 1️⃣ **PySpark-01-Read-DataFiles.ipynb**
**Purpose**: Master different data reading techniques and formats

**Topics Covered:**
- ✅ Reading CSV files with schema inference
- ✅ Reading JSON files (single-line and multi-line)
- ✅ Reading Parquet files
- ✅ Reading Delta Lake tables
- ✅ Reading from Azure Data Lake Storage Gen2
- ✅ Schema specification and validation
- ✅ Header handling and delimiter options
- ✅ Handling corrupted records

**Key Concepts:**

#### **CSV Reading**
```python
# Basic CSV read with schema inference
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("path/to/file.csv")

# CSV read with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("DATE_OF_PRICING", StringType(), True),
    StructField("STATE_NAME", StringType(), True),
    StructField("MODAL_PRICE", DoubleType(), True)
])

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("path/to/file.csv")
```

#### **JSON Reading**
```python
# Single-line JSON
df = spark.read.format("json").load("path/to/file.json")

# Multi-line JSON
df = spark.read.format("json") \
    .option("multiline", "true") \
    .load("path/to/file.json")
```

#### **Parquet Reading**
```python
# Parquet (columnar format, schema preserved)
df = spark.read.format("parquet").load("path/to/file.parquet")

# Parquet with partition pruning
df = spark.read.format("parquet") \
    .load("path/to/data/") \
    .filter("year = 2023 AND month = 10")
```

#### **Delta Lake Reading**
```python
# Delta table read
df = spark.read.format("delta").load("path/to/delta/table")

# Time travel
df = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("path/to/delta/table")

# Read from catalog
df = spark.table("pricing_analytics.silver.daily_pricing_silver")
```

#### **ADLS Gen2 Reading**
```python
# Configure ADLS access
storage_account = "yourstorageaccount"
container = "bronze"
storage_key = "your_storage_key"

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Read from ADLS
df = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"abfss://{container}@{storage_account}.dfs.core.windows.net/daily-pricing/")
```

**Use Cases:**
- Initial data loading
- Data migration from various sources
- ETL pipeline starting points
- Data validation and profiling

---

### 2️⃣ **PySpark-02-Select-DataFrame-Operations.ipynb**
**Purpose**: Learn column selection, filtering, and basic DataFrame operations

**Topics Covered:**
- ✅ Column selection techniques
- ✅ Adding and dropping columns
- ✅ Column renaming
- ✅ withColumn() transformations
- ✅ select() vs selectExpr()
- ✅ Column aliasing
- ✅ Casting data types

**Key Operations:**

#### **Column Selection**
```python
# Select specific columns
df.select("STATE_NAME", "MARKET_NAME", "MODAL_PRICE")

# Select using column objects
from pyspark.sql.functions import col
df.select(col("STATE_NAME"), col("MODAL_PRICE"))

# Select all columns except some
df.drop("unwanted_column1", "unwanted_column2")

# Select with expressions
df.selectExpr("STATE_NAME", "MODAL_PRICE * 1.1 as adjusted_price")
```

#### **Adding Columns**
```python
from pyspark.sql.functions import lit, current_timestamp, concat

# Add literal column
df = df.withColumn("country", lit("India"))

# Add calculated column
df = df.withColumn("price_range", col("MAXIMUM_PRICE") - col("MINIMUM_PRICE"))

# Add timestamp column
df = df.withColumn("processed_at", current_timestamp())

# Concatenate columns
df = df.withColumn("location", concat(col("STATE_NAME"), lit(" - "), col("MARKET_NAME")))
```

#### **Column Renaming**
```python
# Rename single column
df = df.withColumnRenamed("MODAL_PRICE", "modal_price")

# Rename multiple columns
df = df.toDF("date", "state", "market", "product", "price")

# Alias in select
df.select(col("MODAL_PRICE").alias("price"))
```

#### **Type Casting**
```python
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, DateType

# Cast columns
df = df.withColumn("MODAL_PRICE", col("MODAL_PRICE").cast(DoubleType()))
df = df.withColumn("ROW_ID", col("ROW_ID").cast(IntegerType()))
df = df.withColumn("DATE_OF_PRICING", col("DATE_OF_PRICING").cast(DateType()))
```

**Best Practices:**
- Use `select()` for column subsetting
- Use `withColumn()` for adding/transforming columns
- Chain operations for readability
- Avoid repeated `withColumn()` calls (use `select()` with multiple expressions instead)

---

### 3️⃣ **PySpark-03-Filter-Where.ipynb**
**Purpose**: Master data filtering techniques

**Topics Covered:**
- ✅ filter() vs where() (they're equivalent)
- ✅ Multiple filter conditions
- ✅ Complex boolean logic
- ✅ NULL handling in filters
- ✅ String filtering with pattern matching
- ✅ Date range filtering

**Filter Examples:**

#### **Basic Filtering**
```python
# Single condition
df.filter(col("STATE_NAME") == "Maharashtra")
df.where("STATE_NAME = 'Maharashtra'")  # SQL-style

# Multiple conditions with AND
df.filter((col("STATE_NAME") == "Maharashtra") & (col("MODAL_PRICE") > 1000))

# Multiple conditions with OR
df.filter((col("STATE_NAME") == "Maharashtra") | (col("STATE_NAME") == "Karnataka"))

# NOT condition
df.filter(~(col("STATE_NAME") == "Maharashtra"))
df.filter(col("STATE_NAME") != "Maharashtra")
```

#### **Advanced Filtering**
```python
from pyspark.sql.functions import col, upper, lower, trim

# IN clause
df.filter(col("STATE_NAME").isin("Maharashtra", "Karnataka", "Tamil Nadu"))

# BETWEEN
df.filter(col("MODAL_PRICE").between(1000, 5000))

# String operations
df.filter(col("PRODUCT_NAME").like("%gourd%"))
df.filter(col("PRODUCT_NAME").startswith("Little"))
df.filter(col("PRODUCT_NAME").endswith("(Kundru)"))
df.filter(col("PRODUCT_NAME").contains("gourd"))

# Case-insensitive filtering
df.filter(upper(col("STATE_NAME")) == "MAHARASHTRA")

# NULL filtering
df.filter(col("MODAL_PRICE").isNotNull())
df.filter(col("MODAL_PRICE").isNull())
```

#### **Date Filtering**
```python
from pyspark.sql.functions import to_date, year, month, dayofmonth

# Date comparisons
df.filter(col("DATE_OF_PRICING") >= "2023-01-01")
df.filter(col("DATE_OF_PRICING").between("2023-01-01", "2023-12-31"))

# Extract date parts
df.filter(year(col("DATE_OF_PRICING")) == 2023)
df.filter(month(col("DATE_OF_PRICING")) == 1)

# Current date comparisons
from pyspark.sql.functions import current_date, date_sub
df.filter(col("DATE_OF_PRICING") >= date_sub(current_date(), 30))  # Last 30 days
```

#### **Complex Conditions**
```python
# Nested conditions
df.filter(
    ((col("STATE_NAME") == "Maharashtra") & (col("MODAL_PRICE") > 1000)) |
    ((col("STATE_NAME") == "Karnataka") & (col("MODAL_PRICE") > 800))
)

# Chained filters (better performance)
df.filter(col("STATE_NAME") == "Maharashtra") \
  .filter(col("MODAL_PRICE") > 1000) \
  .filter(col("PRODUCT_NAME").contains("gourd"))
```

**Performance Tips:**
- Filter early in the transformation pipeline
- Use column pruning with filters
- Leverage partition pruning for partitioned data
- Chain filters for better optimization

---

### 4️⃣ **PySpark-04-Distinct-DropDuplicates.ipynb**
**Purpose**: Handle duplicate records and unique value extraction

**Topics Covered:**
- ✅ distinct() for unique rows
- ✅ dropDuplicates() with specific columns
- ✅ Deduplication strategies
- ✅ Count distinct values
- ✅ Identifying duplicate records

**Deduplication Techniques:**

#### **Get Unique Rows**
```python
# Get all unique rows
df_unique = df.distinct()

# Count unique rows
unique_count = df.distinct().count()
```

#### **Drop Duplicates by Columns**
```python
# Drop duplicates considering all columns
df_dedup = df.dropDuplicates()

# Drop duplicates based on specific columns
df_dedup = df.dropDuplicates(["STATE_NAME", "MARKET_NAME", "DATE_OF_PRICING"])

# Keep first occurrence (default)
df_dedup = df.dropDuplicates(["STATE_NAME", "MARKET_NAME"])
```

#### **Identify Duplicates**
```python
from pyspark.sql.functions import count
from pyspark.sql.window import Window

# Find duplicate counts
duplicate_df = df.groupBy("STATE_NAME", "MARKET_NAME", "DATE_OF_PRICING") \
    .agg(count("*").alias("duplicate_count")) \
    .filter(col("duplicate_count") > 1)

# Add row number to identify duplicates
windowSpec = Window.partitionBy("STATE_NAME", "MARKET_NAME").orderBy("DATE_OF_PRICING")
df_with_rownum = df.withColumn("row_num", row_number().over(windowSpec))

# Keep only first occurrence
df_dedup = df_with_rownum.filter(col("row_num") == 1).drop("row_num")
```

#### **Count Distinct**
```python
from pyspark.sql.functions import countDistinct

# Count distinct values
df.select(countDistinct("STATE_NAME").alias("unique_states")).show()

# Multiple distinct counts
df.agg(
    countDistinct("STATE_NAME").alias("unique_states"),
    countDistinct("MARKET_NAME").alias("unique_markets"),
    countDistinct("PRODUCT_NAME").alias("unique_products")
).show()
```

**Use Cases:**
- Data quality improvement
- Master data management
- Dimension table creation
- Analytical aggregations

---

### 5️⃣ **PySpark-05-Sort-OrderBy.ipynb**
**Purpose**: Learn data sorting and ordering techniques

**Topics Covered:**
- ✅ sort() vs orderBy() (equivalent)
- ✅ Ascending and descending order
- ✅ Multi-column sorting
- ✅ NULL ordering
- ✅ Performance considerations

**Sorting Examples:**

#### **Basic Sorting**
```python
from pyspark.sql.functions import col, asc, desc

# Ascending order (default)
df.sort("MODAL_PRICE")
df.orderBy("MODAL_PRICE")
df.orderBy(col("MODAL_PRICE").asc())

# Descending order
df.sort(col("MODAL_PRICE").desc())
df.orderBy(col("MODAL_PRICE").desc())
```

#### **Multi-Column Sorting**
```python
# Sort by multiple columns
df.orderBy("STATE_NAME", "MARKET_NAME")

# Mixed ascending/descending
df.orderBy(col("STATE_NAME").asc(), col("MODAL_PRICE").desc())

# Alternative syntax
df.sort(["STATE_NAME", "MODAL_PRICE"], ascending=[True, False])
```

#### **NULL Handling**
```python
# NULLs first
df.orderBy(col("MODAL_PRICE").asc_nulls_first())

# NULLs last (default for ascending)
df.orderBy(col("MODAL_PRICE").asc_nulls_last())

# Descending with NULLs last
df.orderBy(col("MODAL_PRICE").desc_nulls_last())
```

#### **Performance Optimization**
```python
# Use coalesce for small result sets
df.coalesce(1).orderBy("MODAL_PRICE")  # Single partition for full sort

# Use sortWithinPartitions for distributed sorting
df.sortWithinPartitions("STATE_NAME")  # Sort within each partition

# Limit after sort (more efficient)
df.orderBy(col("MODAL_PRICE").desc()).limit(10)  # Top 10
```

**Performance Tips:**
- Avoid global sorting on large datasets
- Use `sortWithinPartitions()` when full ordering isn't needed
- Combine with `limit()` for top-N queries
- Consider partitioning before sorting

---

### 6️⃣ **PySpark-06-GroupBy-Aggregations.ipynb**
**Purpose**: Master aggregation operations and group-by logic

**Topics Covered:**
- ✅ Basic aggregations (sum, avg, min, max, count)
- ✅ groupBy() with multiple columns
- ✅ Custom aggregation functions
- ✅ Having clause equivalent
- ✅ Pivot operations
- ✅ Rollup and cube

**Aggregation Examples:**

#### **Basic Aggregations**
```python
from pyspark.sql.functions import sum, avg, min, max, count, countDistinct

# Simple aggregation
df.agg(
    sum("ARRIVAL_IN_TONNES").alias("total_arrival"),
    avg("MODAL_PRICE").alias("avg_price"),
    min("MINIMUM_PRICE").alias("min_price"),
    max("MAXIMUM_PRICE").alias("max_price"),
    count("*").alias("record_count")
).show()
```

#### **Group By Aggregations**
```python
# Group by single column
df.groupBy("STATE_NAME") \
    .agg(
        sum("ARRIVAL_IN_TONNES").alias("state_total_arrival"),
        avg("MODAL_PRICE").alias("state_avg_price")
    ).show()

# Group by multiple columns
df.groupBy("STATE_NAME", "PRODUCT_NAME") \
    .agg(
        count("*").alias("record_count"),
        avg("MODAL_PRICE").alias("avg_price"),
        min("MINIMUM_PRICE").alias("min_price"),
        max("MAXIMUM_PRICE").alias("max_price")
    ).show()
```

#### **Multiple Aggregations**
```python
# Multiple metrics per group
state_summary = df.groupBy("STATE_NAME").agg(
    count("*").alias("total_records"),
    countDistinct("MARKET_NAME").alias("unique_markets"),
    countDistinct("PRODUCT_NAME").alias("unique_products"),
    sum("ARRIVAL_IN_TONNES").alias("total_arrival"),
    avg("MODAL_PRICE").alias("avg_price"),
    min("DATE_OF_PRICING").alias("earliest_date"),
    max("DATE_OF_PRICING").alias("latest_date")
)
```

#### **Having Clause (Filter After Aggregation)**
```python
# Filter aggregated results
df.groupBy("STATE_NAME") \
    .agg(avg("MODAL_PRICE").alias("avg_price")) \
    .filter(col("avg_price") > 1000) \
    .show()

# Multiple having conditions
df.groupBy("PRODUCT_NAME") \
    .agg(
        count("*").alias("record_count"),
        avg("MODAL_PRICE").alias("avg_price")
    ) \
    .filter((col("record_count") > 100) & (col("avg_price") > 500)) \
    .show()
```

#### **Pivot Operations**
```python
# Pivot state by product to see avg prices
pivot_df = df.groupBy("STATE_NAME") \
    .pivot("PRODUCT_NAME") \
    .agg(avg("MODAL_PRICE"))

# Pivot with multiple aggregations
pivot_multi = df.groupBy("STATE_NAME") \
    .pivot("PRODUCT_NAME") \
    .agg(
        avg("MODAL_PRICE").alias("avg_price"),
        sum("ARRIVAL_IN_TONNES").alias("total_arrival")
    )
```

#### **Rollup and Cube**
```python
# Rollup - hierarchical aggregations
rollup_df = df.rollup("STATE_NAME", "MARKET_NAME") \
    .agg(sum("ARRIVAL_IN_TONNES").alias("total_arrival"))

# Cube - all possible combinations
cube_df = df.cube("STATE_NAME", "PRODUCT_NAME", "VARIETY") \
    .agg(avg("MODAL_PRICE").alias("avg_price"))
```

**Custom Aggregations:**
```python
from pyspark.sql.functions import expr

# Weighted average
df.groupBy("STATE_NAME").agg(
    expr("sum(MODAL_PRICE * ARRIVAL_IN_TONNES) / sum(ARRIVAL_IN_TONNES)").alias("weighted_avg_price")
)

# Collect list of values
df.groupBy("STATE_NAME").agg(
    collect_list("MARKET_NAME").alias("markets"),
    collect_set("PRODUCT_NAME").alias("unique_products")
)
```

---

### 7️⃣ **PySpark-07-Window-Functions.ipynb**
**Purpose**: Master window functions for advanced analytics

**Topics Covered:**
- ✅ Window specification (partitionBy, orderBy)
- ✅ Ranking functions (row_number, rank, dense_rank)
- ✅ Analytical functions (lead, lag)
- ✅ Aggregate window functions
- ✅ Running totals and moving averages
- ✅ Frame specifications

**Window Function Examples:**

#### **Window Specification**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

# Define window
windowSpec = Window.partitionBy("STATE_NAME").orderBy(col("MODAL_PRICE").desc())
```

#### **Ranking Functions**
```python
# Row number - unique sequential number
df_ranked = df.withColumn("row_num", row_number().over(windowSpec))

# Rank - gaps in ranking for ties
df_ranked = df.withColumn("rank", rank().over(windowSpec))

# Dense rank - no gaps in ranking
df_ranked = df.withColumn("dense_rank", dense_rank().over(windowSpec))

# Example: Top 3 products by price per state
top_products = df.withColumn(
    "price_rank",
    row_number().over(
        Window.partitionBy("STATE_NAME", "PRODUCT_NAME")
              .orderBy(col("MODAL_PRICE").desc())
    )
).filter(col("price_rank") <= 3)
```

#### **Analytical Functions**
```python
from pyspark.sql.functions import lead, lag

# Lead - next row value
df_with_lead = df.withColumn(
    "next_price",
    lead("MODAL_PRICE", 1).over(
        Window.partitionBy("PRODUCT_NAME").orderBy("DATE_OF_PRICING")
    )
)

# Lag - previous row value
df_with_lag = df.withColumn(
    "prev_price",
    lag("MODAL_PRICE", 1).over(
        Window.partitionBy("PRODUCT_NAME").orderBy("DATE_OF_PRICING")
    )
)

# Calculate price change
df_price_change = df.withColumn(
    "price_change",
    col("MODAL_PRICE") - lag("MODAL_PRICE", 1).over(
        Window.partitionBy("PRODUCT_NAME").orderBy("DATE_OF_PRICING")
    )
)
```

#### **Running Totals**
```python
from pyspark.sql.functions import sum as _sum

# Running total
windowSpec = Window.partitionBy("PRODUCT_NAME") \
    .orderBy("DATE_OF_PRICING") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running_total = df.withColumn(
    "cumulative_arrival",
    _sum("ARRIVAL_IN_TONNES").over(windowSpec)
)
```

#### **Moving Averages**
```python
# 7-day moving average
windowSpec_7day = Window.partitionBy("PRODUCT_NAME") \
    .orderBy("DATE_OF_PRICING") \
    .rowsBetween(-6, 0)  # Current row and 6 preceding rows

df_ma = df.withColumn(
    "moving_avg_7day",
    avg("MODAL_PRICE").over(windowSpec_7day)
)

# 30-day moving average
windowSpec_30day = Window.partitionBy("PRODUCT_NAME") \
    .orderBy("DATE_OF_PRICING") \
    .rowsBetween(-29, 0)

df_ma = df.withColumn(
    "moving_avg_30day",
    avg("MODAL_PRICE").over(windowSpec_30day)
)
```

#### **Percentile and Ntile**
```python
from pyspark.sql.functions import percent_rank, ntile

# Percent rank
df_pct = df.withColumn(
    "price_percentile",
    percent_rank().over(Window.orderBy("MODAL_PRICE"))
)

# Ntile - divide into N buckets
df_ntile = df.withColumn(
    "price_quartile",
    ntile(4).over(Window.orderBy("MODAL_PRICE"))
)
```

#### **Aggregate Window Functions**
```python
# Window aggregations without rowsBetween
windowSpec = Window.partitionBy("STATE_NAME")

df_window_agg = df.withColumn(
    "state_avg_price", avg("MODAL_PRICE").over(windowSpec)
).withColumn(
    "state_max_price", max("MODAL_PRICE").over(windowSpec)
).withColumn(
    "price_vs_state_avg", 
    col("MODAL_PRICE") - col("state_avg_price")
)
```

**Use Cases:**
- Time-series analysis
- Ranking and top-N queries
- Trend analysis
- Comparative analytics
- Running calculations

---

### 8️⃣ **PySpark-08-Joins.ipynb**
**Purpose**: Master various join operations

**Topics Covered:**
- ✅ Inner join
- ✅ Left outer join
- ✅ Right outer join
- ✅ Full outer join
- ✅ Cross join
- ✅ Semi join and anti join
- ✅ Join conditions and optimizations
- ✅ Broadcast joins

**Join Examples:**

#### **Basic Joins**
```python
# Inner join
result = df_pricing.join(
    df_market,
    df_pricing["MARKET_NAME"] == df_market["MARKET_NAME"],
    "inner"
)

# Left outer join
result = df_pricing.join(
    df_market,
    "MARKET_NAME",  # Can use column name if same in both
    "left"
)

# Right outer join
result = df_pricing.join(df_market, "MARKET_NAME", "right")

# Full outer join
result = df_pricing.join(df_market, "MARKET_NAME", "full")
```

#### **Multiple Column Joins**
```python
# Join on multiple columns
result = df_fact.join(
    df_dim,
    (df_fact["STATE_NAME"] == df_dim["STATE_NAME"]) &
    (df_fact["MARKET_NAME"] == df_dim["MARKET_NAME"]),
    "inner"
)

# Using list of columns (must have same names)
result = df_fact.join(df_dim, ["STATE_NAME", "MARKET_NAME"], "inner")
```

#### **Semi and Anti Joins**
```python
# Left semi join - returns only matching left records
result = df_pricing.join(
    df_expensive_products,
    "PRODUCT_NAME",
    "left_semi"
)

# Left anti join - returns only non-matching left records
result = df_pricing.join(
    df_excluded_products,
    "PRODUCT_NAME",
    "left_anti"
)
```

#### **Broadcast Join (Performance Optimization)**
```python
from pyspark.sql.functions import broadcast

# Broadcast smaller table to all executors
result = df_large.join(
    broadcast(df_small),
    "PRODUCT_ID",
    "inner"
)
```

#### **Handling Duplicate Column Names**
```python
# Use aliases to avoid ambiguity
df_pricing_alias = df_pricing.alias("pricing")
df_market_alias = df_market.alias("market")

result = df_pricing_alias.join(
    df_market_alias,
    col("pricing.MARKET_NAME") == col("market.MARKET_NAME"),
    "inner"
).select(
    "pricing.*",
    col("market.latitude"),
    col("market.longitude")
)
```

**Join Performance Tips:**
- Broadcast small tables (<10MB)
- Use column pruning before joins
- Filter data before joining
- Consider partitioning strategy
- Use salting for skewed joins

---

### 9️⃣ **PySpark-09-Union-UnionAll.ipynb**
**Purpose**: Combine datasets vertically

**Topics Covered:**
- ✅ union() vs unionAll()
- ✅ unionByName() for schema matching
- ✅ Handling schema differences
- ✅ Deduplication after union

**Union Examples:**

#### **Basic Union**
```python
# Union - removes duplicates (in Spark 3.x, same as unionAll)
result = df1.union(df2)

# UnionAll - keeps all records including duplicates (deprecated, use union)
result = df1.unionAll(df2)  # Deprecated
result = df1.union(df2)  # Preferred
```

#### **Union by Name**
```python
# Union by column names (handles different column orders)
result = df1.unionByName(df2)

# Union by name with missing columns filled as NULL
result = df1.unionByName(df2, allowMissingColumns=True)
```

#### **Multiple DataFrames**
```python
from functools import reduce

# Union multiple DataFrames
dfs = [df1, df2, df3, df4]
result = reduce(lambda df1, df2: df1.union(df2), dfs)
```

#### **Deduplicate After Union**
```python
# Union and remove duplicates
result = df1.union(df2).distinct()

# Union and remove duplicates by specific columns
result = df1.union(df2).dropDuplicates(["STATE_NAME", "MARKET_NAME", "DATE_OF_PRICING"])
```

**Use Cases:**
- Combining data from multiple sources
- Historical data merging
- Incremental data appends
- Multi-region data consolidation

---

### 🔟 **PySpark-10-UDF-Functions.ipynb**
**Purpose**: Create custom user-defined functions

**Topics Covered:**
- ✅ Creating Python UDFs
- ✅ Registering UDFs for SQL
- ✅ Return type specification
- ✅ Vectorized UDFs (pandas_udf)
- ✅ Performance considerations

**UDF Examples:**

#### **Basic Python UDF**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Define Python function
def categorize_price(price):
    if price is None:
        return "Unknown"
    elif price < 500:
        return "Low"
    elif price < 2000:
        return "Medium"
    else:
        return "High"

# Register as UDF
categorize_price_udf = udf(categorize_price, StringType())

# Use UDF
df_categorized = df.withColumn(
    "price_category",
    categorize_price_udf(col("MODAL_PRICE"))
)
```

#### **UDF with Multiple Parameters**
```python
def calculate_premium(min_price, max_price):
    if min_price is None or max_price is None:
        return None
    return (max_price - min_price) / min_price * 100

premium_udf = udf(calculate_premium, DoubleType())

df_premium = df.withColumn(
    "price_premium_pct",
    premium_udf(col("MINIMUM_PRICE"), col("MAXIMUM_PRICE"))
)
```

#### **Register UDF for SQL**
```python
# Register for use in SQL queries
spark.udf.register("categorize_price_sql", categorize_price, StringType())

# Use in SQL
spark.sql("""
    SELECT 
        PRODUCT_NAME,
        MODAL_PRICE,
        categorize_price_sql(MODAL_PRICE) as price_category
    FROM pricing_table
""").show()
```

#### **Pandas UDF (Vectorized - Better Performance)**
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Vectorized UDF using pandas
@pandas_udf(StringType())
def categorize_price_vectorized(prices: pd.Series) -> pd.Series:
    def categorize(price):
        if pd.isna(price):
            return "Unknown"
        elif price < 500:
            return "Low"
        elif price < 2000:
            return "Medium"
        else:
            return "High"
    
    return prices.apply(categorize)

# Use vectorized UDF
df_categorized = df.withColumn(
    "price_category",
    categorize_price_vectorized(col("MODAL_PRICE"))
)
```

#### **Complex Return Types**
```python
from pyspark.sql.types import StructType, StructField

# UDF returning struct
schema = StructType([
    StructField("category", StringType()),
    StructField("is_high_value", BooleanType())
])

@udf(returnType=schema)
def analyze_price(price):
    category = "Low" if price < 500 else "Medium" if price < 2000 else "High"
    is_high_value = price >= 2000
    return (category, is_high_value)

df_analyzed = df.withColumn("price_analysis", analyze_price(col("MODAL_PRICE")))
```

**Performance Tips:**
- Prefer built-in functions over UDFs
- Use pandas_udf for better performance
- Avoid UDFs with complex logic
- Cache results if UDF is expensive

---

## 📊 Complete Example Workflow

Here's a complete example combining multiple concepts:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder \
    .appName("PySpark Complete Example") \
    .getOrCreate()

# 1. Read data
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storage.dfs.core.windows.net/daily-pricing/")

# 2. Data cleansing
df_clean = df \
    .filter(col("MODAL_PRICE").isNotNull()) \
    .withColumn("DATE_OF_PRICING", to_date(col("DATE_OF_PRICING"))) \
    .withColumn("MODAL_PRICE", col("MODAL_PRICE").cast("double")) \
    .dropDuplicates(["STATE_NAME", "MARKET_NAME", "DATE_OF_PRICING", "PRODUCT_NAME"])

# 3. Add calculated columns
df_enhanced = df_clean \
    .withColumn("YEAR", year(col("DATE_OF_PRICING"))) \
    .withColumn("MONTH", month(col("DATE_OF_PRICING"))) \
    .withColumn("PRICE_RANGE", col("MAXIMUM_PRICE") - col("MINIMUM_PRICE"))

# 4. Window functions - price trends
windowSpec = Window.partitionBy("PRODUCT_NAME").orderBy("DATE_OF_PRICING")

df_trends = df_enhanced \
    .withColumn("PREV_PRICE", lag("MODAL_PRICE", 1).over(windowSpec)) \
    .withColumn("PRICE_CHANGE", col("MODAL_PRICE") - col("PREV_PRICE")) \
    .withColumn("PRICE_CHANGE_PCT", 
        (col("PRICE_CHANGE") / col("PREV_PRICE")) * 100)

# 5. Aggregations
df_summary = df_trends.groupBy("STATE_NAME", "PRODUCT_NAME") \
    .agg(
        count("*").alias("record_count"),
        avg("MODAL_PRICE").alias("avg_price"),
        min("MODAL_PRICE").alias("min_price"),
        max("MODAL_PRICE").alias("max_price"),
        stddev("MODAL_PRICE").alias("price_std_dev")
    ) \
    .filter(col("record_count") > 50)

# 6. Join with dimension
df_market = spark.table("pricing_analytics.gold.reporting_dim_market_gold")

df_final = df_summary.join(
    broadcast(df_market),
    "MARKET_NAME",
    "left"
)

# 7. Write result
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://gold@storage.dfs.core.windows.net/price_analysis/")

print(f"Processed {df_final.count()} records")
```

---

## 🚀 Performance Best Practices

### 1. **Data Skipping**
```python
# Enable data skipping
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "32")

# Use Z-ordering
spark.sql("OPTIMIZE delta_table ZORDER BY (STATE_NAME, PRODUCT_NAME)")
```

### 2. **Caching**
```python
# Cache frequently accessed DataFrames
df_cached = df.cache()
df_cached.count()  # Trigger caching

# Unpersist when done
df_cached.unpersist()
```

### 3. **Partitioning**
```python
# Repartition for better parallelism
df_repartitioned = df.repartition(200, "STATE_NAME")

# Coalesce to reduce partitions
df_coalesced = df.coalesce(10)
```

### 4. **Column Pruning**
```python
# Select only needed columns early
df_subset = df.select("STATE_NAME", "PRODUCT_NAME", "MODAL_PRICE") \
    .filter(col("MODAL_PRICE") > 1000)
```

### 5. **Predicate Pushdown**
```python
# Filter data as early as possible
df_filtered = spark.read.format("delta") \
    .load("path/to/table") \
    .filter(col("DATE_OF_PRICING") >= "2023-01-01")  # Pushed down to storage
```

---

## 📚 Additional Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks PySpark Guide](https://docs.databricks.com/spark/latest/spark-sql/index.html)
- [PySpark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Delta Lake with PySpark](https://docs.delta.io/latest/quick-start.html)

---

## ✅ Learning Checklist

After completing these notebooks, you should be able to:

- [ ] Read data from various formats and sources
- [ ] Perform column selection and transformations
- [ ] Filter data using complex conditions
- [ ] Handle duplicate records effectively
- [ ] Sort and order data efficiently
- [ ] Aggregate data using groupBy
- [ ] Use window functions for analytics
- [ ] Join datasets using various strategies
- [ ] Combine DataFrames with union operations
- [ ] Create and use custom UDFs
- [ ] Optimize Spark jobs for performance
- [ ] Write efficient PySpark code

---

**🔗 Next Steps:** Apply these PySpark concepts in the [Transformation](../02-Transformation/README.md) and [Delta-ELT-Pipeline](../Delta-ELT-Pipeline/README.md) folders.

---

**📧 Questions or Issues?** Check the main project [README](../README.md) for support information.
