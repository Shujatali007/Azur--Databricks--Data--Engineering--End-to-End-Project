# ⚡ Apache Spark SQL Fundamentals

This folder contains comprehensive Spark SQL tutorials covering fundamental to advanced SQL operations in Databricks. These notebooks demonstrate SQL-based data processing, table management, and query optimization techniques.

## 📋 Overview

Spark SQL provides a SQL interface to Spark's distributed data processing capabilities. This collection demonstrates:
- Table creation and management (Managed & External)
- SQL query patterns and optimization
- Data manipulation with SQL
- DateTime operations and functions
- Integration with Delta Lake
- Best practices for production workloads

---

## 📂 Files in This Folder

### 1️⃣ **01-Notebook-Introduction.py**
**Purpose**: Introduction to Databricks notebooks and basic Spark SQL concepts

**Topics Covered:**
- ✅ Notebook fundamentals
- ✅ Magic commands (%sql, %python, %md, %sh, %fs)
- ✅ Cell execution and output
- ✅ Databricks UI navigation
- ✅ Basic SQL queries
- ✅ display() function usage

**Key Concepts:**

#### **Magic Commands**
```python
# MAGIC %md
# MAGIC # Markdown cells for documentation
# MAGIC Use markdown syntax for formatted text, links, images, etc.

# MAGIC %sql
# MAGIC -- SQL cells for direct SQL execution
# MAGIC SELECT * FROM table_name LIMIT 10

# MAGIC %python
# MAGIC # Python code execution
# MAGIC df = spark.table("table_name")
# MAGIC display(df)

# MAGIC %sh
# MAGIC # Shell commands
# MAGIC ls -la /dbfs/mnt/

# MAGIC %fs
# MAGIC # File system commands
# MAGIC ls /mnt/bronze/
```

#### **Basic Notebook Operations**
```python
# Display data
display(spark.sql("SELECT * FROM pricing_analytics.silver.daily_pricing_silver LIMIT 100"))

# Create temporary views
spark.sql("""
    CREATE OR REPLACE TEMP VIEW temp_pricing AS
    SELECT * FROM pricing_analytics.silver.daily_pricing_silver
    WHERE STATE_NAME = 'Maharashtra'
""")

# Use Python and SQL together
state_name = "Maharashtra"
spark.sql(f"SELECT * FROM temp_pricing WHERE STATE_NAME = '{state_name}'")
```

**Use Cases:**
- Learning Databricks environment
- Understanding notebook structure
- Quick data exploration
- Prototyping queries

---

### 2️⃣ **02-Spark-SQL-Managed-Tables.sql**
**Purpose**: Create and manage Spark managed tables

**Topics Covered:**
- ✅ Managed table creation
- ✅ Table properties and metadata
- ✅ Data insertion patterns
- ✅ Table partitioning
- ✅ Table optimization
- ✅ Lifecycle management

**Managed Tables Overview:**

Managed tables (also called internal tables) are tables where Spark manages both the **metadata** and the **data files**. When you drop a managed table, both metadata and data are deleted.

#### **Creating Managed Tables**

```sql
-- Create managed table with schema
CREATE TABLE IF NOT EXISTS pricing_analytics.silver.daily_pricing_managed (
    DATE_OF_PRICING DATE,
    STATE_NAME STRING,
    MARKET_NAME STRING,
    PRODUCT_NAME STRING,
    PRODUCTGROUP_NAME STRING,
    VARIETY STRING,
    ARRIVAL_IN_TONNES DECIMAL(10,2),
    MINIMUM_PRICE DECIMAL(10,2),
    MAXIMUM_PRICE DECIMAL(10,2),
    MODAL_PRICE DECIMAL(10,2),
    lakehouse_inserted_date TIMESTAMP,
    lakehouse_updated_date TIMESTAMP
)
USING DELTA
COMMENT 'Managed table for daily pricing data';
```

#### **Create Table As Select (CTAS)**

```sql
-- Create managed table from query results
CREATE TABLE pricing_analytics.silver.maharashtra_pricing
AS
SELECT 
    DATE_OF_PRICING,
    MARKET_NAME,
    PRODUCT_NAME,
    MODAL_PRICE
FROM pricing_analytics.silver.daily_pricing_silver
WHERE STATE_NAME = 'Maharashtra';
```

#### **Partitioned Managed Tables**

```sql
-- Create partitioned managed table for better query performance
CREATE TABLE pricing_analytics.silver.daily_pricing_partitioned (
    DATE_OF_PRICING DATE,
    STATE_NAME STRING,
    MARKET_NAME STRING,
    PRODUCT_NAME STRING,
    MODAL_PRICE DECIMAL(10,2)
)
USING DELTA
PARTITIONED BY (STATE_NAME, year(DATE_OF_PRICING))
COMMENT 'Partitioned by state and year';
```

#### **Insert Data Patterns**

```sql
-- Insert from SELECT
INSERT INTO pricing_analytics.silver.daily_pricing_managed
SELECT * FROM pricing_analytics.bronze.daily_pricing
WHERE DATE_OF_PRICING = '2023-10-29';

-- Insert with VALUES
INSERT INTO pricing_analytics.silver.daily_pricing_managed VALUES
    ('2023-10-29', 'Maharashtra', 'Pune', 'Tomato', 'Vegetables', 'Local', 5.5, 20.00, 35.00, 28.00, current_timestamp(), current_timestamp());

-- Insert Overwrite (replace all data)
INSERT OVERWRITE TABLE pricing_analytics.silver.daily_pricing_managed
SELECT * FROM pricing_analytics.bronze.daily_pricing;
```

#### **Table Properties**

```sql
-- Set table properties
ALTER TABLE pricing_analytics.silver.daily_pricing_managed
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- View table properties
DESCRIBE EXTENDED pricing_analytics.silver.daily_pricing_managed;

-- Show table details
SHOW CREATE TABLE pricing_analytics.silver.daily_pricing_managed;
```

#### **Table Management**

```sql
-- View table information
DESCRIBE TABLE pricing_analytics.silver.daily_pricing_managed;
DESCRIBE EXTENDED pricing_analytics.silver.daily_pricing_managed;

-- Show table columns
SHOW COLUMNS IN pricing_analytics.silver.daily_pricing_managed;

-- Show partitions
SHOW PARTITIONS pricing_analytics.silver.daily_pricing_partitioned;

-- Analyze table for statistics
ANALYZE TABLE pricing_analytics.silver.daily_pricing_managed COMPUTE STATISTICS;

-- Optimize table (compact small files)
OPTIMIZE pricing_analytics.silver.daily_pricing_managed;

-- Z-Order for better query performance
OPTIMIZE pricing_analytics.silver.daily_pricing_managed
ZORDER BY (STATE_NAME, PRODUCT_NAME);

-- Vacuum old files (after retention period)
VACUUM pricing_analytics.silver.daily_pricing_managed RETAIN 168 HOURS;

-- Drop table (deletes both metadata and data)
DROP TABLE IF EXISTS pricing_analytics.silver.daily_pricing_managed;
```

**Advantages of Managed Tables:**
- ✅ Spark handles data lifecycle
- ✅ Simple to create and manage
- ✅ Automatic cleanup when dropped
- ✅ Good for temporary/intermediate data

**When to Use:**
- Data owned by Spark/Databricks
- No external dependencies
- Development and testing
- Intermediate processing results

---

### 3️⃣ **03-Spark-SQL-External-Tables.sql**
**Purpose**: Create and manage external tables pointing to external storage

**Topics Covered:**
- ✅ External table creation
- ✅ Location specification
- ✅ Schema-on-read patterns
- ✅ Data independence
- ✅ External data access
- ✅ Metastore registration

**External Tables Overview:**

External tables (also called unmanaged tables) store metadata in the metastore while data remains in an external location. Dropping an external table only removes metadata; data files remain intact.

#### **Creating External Tables**

```sql
-- Create external table pointing to ADLS Gen2
CREATE TABLE IF NOT EXISTS pricing_analytics.bronze.daily_pricing_external (
    DATE_OF_PRICING STRING,
    STATE_NAME STRING,
    MARKET_NAME STRING,
    PRODUCT_NAME STRING,
    PRODUCTGROUP_NAME STRING,
    VARIETY STRING,
    ORIGIN STRING,
    ARRIVAL_IN_TONNES STRING,
    MINIMUM_PRICE STRING,
    MAXIMUM_PRICE STRING,
    MODAL_PRICE STRING
)
USING CSV
OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'path' = 'abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing/'
)
COMMENT 'External table for CSV files in Bronze layer';
```

#### **External Delta Tables**

```sql
-- Create external Delta table
CREATE TABLE IF NOT EXISTS pricing_analytics.silver.daily_pricing_external_delta (
    DATE_OF_PRICING DATE,
    STATE_NAME STRING,
    MARKET_NAME STRING,
    PRODUCT_NAME STRING,
    MODAL_PRICE DECIMAL(10,2)
)
USING DELTA
LOCATION 'abfss://silver@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-external/';
```

#### **Register Existing Data as External Table**

```sql
-- Register existing Delta table in storage
CREATE TABLE IF NOT EXISTS pricing_analytics.silver.existing_delta_data
USING DELTA
LOCATION 'abfss://silver@adlsujadatalakehousedev.dfs.core.windows.net/existing-data/';

-- Register Parquet files
CREATE TABLE IF NOT EXISTS pricing_analytics.bronze.parquet_data (
    id BIGINT,
    name STRING,
    value DOUBLE
)
USING PARQUET
LOCATION 'abfss://bronze@storage.dfs.core.windows.net/parquet-files/';

-- Register JSON files
CREATE TABLE IF NOT EXISTS pricing_analytics.bronze.json_data
USING JSON
OPTIONS ('multiline' = 'true')
LOCATION 'abfss://bronze@storage.dfs.core.windows.net/json-files/';
```

#### **Partitioned External Tables**

```sql
-- Create partitioned external table
CREATE TABLE IF NOT EXISTS pricing_analytics.silver.daily_pricing_partitioned_external (
    DATE_OF_PRICING DATE,
    MARKET_NAME STRING,
    PRODUCT_NAME STRING,
    MODAL_PRICE DECIMAL(10,2)
)
USING DELTA
PARTITIONED BY (STATE_NAME, year(DATE_OF_PRICING))
LOCATION 'abfss://silver@storage.dfs.core.windows.net/partitioned-pricing/';

-- Add partition manually (for Hive-style partitioning)
ALTER TABLE pricing_analytics.silver.daily_pricing_partitioned_external
ADD PARTITION (STATE_NAME='Maharashtra', year=2023)
LOCATION 'abfss://silver@storage.dfs.core.windows.net/partitioned-pricing/STATE_NAME=Maharashtra/year=2023/';

-- Repair partitions (auto-discover)
MSCK REPAIR TABLE pricing_analytics.silver.daily_pricing_partitioned_external;
```

#### **External Table Management**

```sql
-- Refresh table metadata (for schema changes)
REFRESH TABLE pricing_analytics.bronze.daily_pricing_external;

-- Update statistics
ANALYZE TABLE pricing_analytics.bronze.daily_pricing_external COMPUTE STATISTICS;

-- View table location
DESCRIBE EXTENDED pricing_analytics.bronze.daily_pricing_external;

-- Drop external table (keeps data files)
DROP TABLE IF EXISTS pricing_analytics.bronze.daily_pricing_external;
-- Data still exists at: abfss://bronze@storage.dfs.core.windows.net/daily-pricing/

-- Re-create table pointing to same location
CREATE TABLE pricing_analytics.bronze.daily_pricing_external
USING CSV
OPTIONS ('header' = 'true', 'inferSchema' = 'true')
LOCATION 'abfss://bronze@storage.dfs.core.windows.net/daily-pricing/';
```

#### **Working with Different File Formats**

```sql
-- CSV External Table
CREATE TABLE bronze.csv_data
USING CSV
OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'delimiter' = ',',
    'dateFormat' = 'dd/MM/yyyy'
)
LOCATION 'abfss://bronze@storage.dfs.core.windows.net/csv-files/';

-- JSON External Table
CREATE TABLE bronze.json_data
USING JSON
OPTIONS (
    'multiline' = 'true',
    'dateFormat' = 'yyyy-MM-dd'
)
LOCATION 'abfss://bronze@storage.dfs.core.windows.net/json-files/';

-- Parquet External Table
CREATE TABLE silver.parquet_data
USING PARQUET
LOCATION 'abfss://silver@storage.dfs.core.windows.net/parquet-files/';

-- Delta External Table (Recommended)
CREATE TABLE gold.delta_data
USING DELTA
LOCATION 'abfss://gold@storage.dfs.core.windows.net/delta-tables/';
```

**Advantages of External Tables:**
- ✅ Data persists after table drop
- ✅ Share data across multiple systems
- ✅ Separate compute from storage
- ✅ Access existing data lakes
- ✅ Better for production workloads

**When to Use:**
- Data shared across multiple platforms
- Production data lakes
- Existing data in storage
- Need data persistence
- Separation of concerns

**Managed vs External Tables Comparison:**

| Feature | Managed Table | External Table |
|---------|---------------|----------------|
| **Data Location** | Spark-managed location | User-specified location |
| **Drop Behavior** | Deletes metadata + data | Deletes metadata only |
| **Use Case** | Temporary/intermediate | Production/shared data |
| **Data Ownership** | Spark/Databricks | External system |
| **Flexibility** | Lower | Higher |
| **Maintenance** | Automatic | Manual |

---

### 4️⃣ **05-Spark-SQL-DateTime-Functions.sql**
**Purpose**: Master date and time operations in Spark SQL

**Topics Covered:**
- ✅ Date/time data types
- ✅ Date extraction functions
- ✅ Date arithmetic
- ✅ Date formatting and parsing
- ✅ Timestamp operations
- ✅ Window functions with dates
- ✅ Time zone handling

**DateTime Function Examples:**

#### **Current Date and Time**

```sql
-- Current date (date type)
SELECT current_date() as today;

-- Current timestamp (timestamp type)
SELECT current_timestamp() as now;

-- Current timezone
SELECT current_timezone() as timezone;
```

#### **Date Extraction Functions**

```sql
-- Extract date parts
SELECT 
    DATE_OF_PRICING,
    year(DATE_OF_PRICING) as year,
    quarter(DATE_OF_PRICING) as quarter,
    month(DATE_OF_PRICING) as month,
    dayofmonth(DATE_OF_PRICING) as day,
    dayofweek(DATE_OF_PRICING) as day_of_week,
    dayofyear(DATE_OF_PRICING) as day_of_year,
    weekofyear(DATE_OF_PRICING) as week_of_year,
    last_day(DATE_OF_PRICING) as last_day_of_month
FROM pricing_analytics.silver.daily_pricing_silver
LIMIT 10;
```

#### **Date Formatting**

```sql
-- Format dates
SELECT 
    DATE_OF_PRICING,
    date_format(DATE_OF_PRICING, 'yyyy-MM-dd') as iso_format,
    date_format(DATE_OF_PRICING, 'dd/MM/yyyy') as uk_format,
    date_format(DATE_OF_PRICING, 'MM-dd-yyyy') as us_format,
    date_format(DATE_OF_PRICING, 'EEEE, MMMM dd, yyyy') as long_format,
    date_format(DATE_OF_PRICING, 'yyyy-MM') as year_month,
    date_format(DATE_OF_PRICING, 'yyyyMM') as year_month_compact
FROM pricing_analytics.silver.daily_pricing_silver
LIMIT 10;
```

#### **Date Parsing**

```sql
-- Parse string to date
SELECT 
    to_date('2023-10-29', 'yyyy-MM-dd') as parsed_date,
    to_date('29/10/2023', 'dd/MM/yyyy') as uk_parsed,
    to_date('10-29-2023', 'MM-dd-yyyy') as us_parsed;

-- Parse string to timestamp
SELECT 
    to_timestamp('2023-10-29 14:30:00', 'yyyy-MM-dd HH:mm:ss') as parsed_timestamp,
    to_timestamp('29/10/2023 14:30', 'dd/MM/yyyy HH:mm') as custom_timestamp;

-- Unix timestamp conversion
SELECT 
    unix_timestamp('2023-10-29 14:30:00') as unix_ts,
    from_unixtime(1698588600) as from_unix;
```

#### **Date Arithmetic**

```sql
-- Add/subtract days
SELECT 
    DATE_OF_PRICING,
    date_add(DATE_OF_PRICING, 7) as next_week,
    date_sub(DATE_OF_PRICING, 7) as last_week,
    add_months(DATE_OF_PRICING, 1) as next_month,
    add_months(DATE_OF_PRICING, -1) as last_month
FROM pricing_analytics.silver.daily_pricing_silver
LIMIT 10;

-- Date difference
SELECT 
    DATE_OF_PRICING,
    datediff(current_date(), DATE_OF_PRICING) as days_ago,
    months_between(current_date(), DATE_OF_PRICING) as months_diff
FROM pricing_analytics.silver.daily_pricing_silver
LIMIT 10;
```

#### **Date Ranges and Filtering**

```sql
-- Filter by date range
SELECT *
FROM pricing_analytics.silver.daily_pricing_silver
WHERE DATE_OF_PRICING BETWEEN '2023-01-01' AND '2023-12-31';

-- Current year data
SELECT *
FROM pricing_analytics.silver.daily_pricing_silver
WHERE year(DATE_OF_PRICING) = year(current_date());

-- Last 30 days
SELECT *
FROM pricing_analytics.silver.daily_pricing_silver
WHERE DATE_OF_PRICING >= date_sub(current_date(), 30);

-- Current month
SELECT *
FROM pricing_analytics.silver.daily_pricing_silver
WHERE year(DATE_OF_PRICING) = year(current_date())
  AND month(DATE_OF_PRICING) = month(current_date());

-- Specific quarter
SELECT *
FROM pricing_analytics.silver.daily_pricing_silver
WHERE year(DATE_OF_PRICING) = 2023
  AND quarter(DATE_OF_PRICING) = 3;
```

#### **Date Aggregations**

```sql
-- Group by year and month
SELECT 
    year(DATE_OF_PRICING) as year,
    month(DATE_OF_PRICING) as month,
    date_format(DATE_OF_PRICING, 'MMMM yyyy') as month_name,
    COUNT(*) as record_count,
    AVG(MODAL_PRICE) as avg_price,
    SUM(ARRIVAL_IN_TONNES) as total_arrival
FROM pricing_analytics.silver.daily_pricing_silver
GROUP BY year(DATE_OF_PRICING), month(DATE_OF_PRICING), date_format(DATE_OF_PRICING, 'MMMM yyyy')
ORDER BY year, month;

-- Group by week
SELECT 
    year(DATE_OF_PRICING) as year,
    weekofyear(DATE_OF_PRICING) as week_number,
    MIN(DATE_OF_PRICING) as week_start,
    MAX(DATE_OF_PRICING) as week_end,
    COUNT(*) as record_count
FROM pricing_analytics.silver.daily_pricing_silver
GROUP BY year(DATE_OF_PRICING), weekofyear(DATE_OF_PRICING)
ORDER BY year, week_number;
```

#### **Window Functions with Dates**

```sql
-- Running totals by date
SELECT 
    DATE_OF_PRICING,
    PRODUCT_NAME,
    ARRIVAL_IN_TONNES,
    SUM(ARRIVAL_IN_TONNES) OVER (
        PARTITION BY PRODUCT_NAME 
        ORDER BY DATE_OF_PRICING 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_arrival
FROM pricing_analytics.silver.daily_pricing_silver
WHERE PRODUCT_NAME = 'Tomato'
ORDER BY DATE_OF_PRICING;

-- Moving average (7-day)
SELECT 
    DATE_OF_PRICING,
    PRODUCT_NAME,
    MODAL_PRICE,
    AVG(MODAL_PRICE) OVER (
        PARTITION BY PRODUCT_NAME 
        ORDER BY DATE_OF_PRICING 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day
FROM pricing_analytics.silver.daily_pricing_silver
WHERE PRODUCT_NAME = 'Tomato'
ORDER BY DATE_OF_PRICING;

-- Year-over-year comparison
SELECT 
    DATE_OF_PRICING,
    PRODUCT_NAME,
    MODAL_PRICE,
    LAG(MODAL_PRICE, 365) OVER (
        PARTITION BY PRODUCT_NAME 
        ORDER BY DATE_OF_PRICING
    ) as price_year_ago,
    MODAL_PRICE - LAG(MODAL_PRICE, 365) OVER (
        PARTITION BY PRODUCT_NAME 
        ORDER BY DATE_OF_PRICING
    ) as yoy_change
FROM pricing_analytics.silver.daily_pricing_silver
WHERE PRODUCT_NAME = 'Tomato'
ORDER BY DATE_OF_PRICING;
```

#### **Date Truncation**

```sql
-- Truncate to different periods
SELECT 
    DATE_OF_PRICING,
    date_trunc('YEAR', DATE_OF_PRICING) as year_start,
    date_trunc('QUARTER', DATE_OF_PRICING) as quarter_start,
    date_trunc('MONTH', DATE_OF_PRICING) as month_start,
    date_trunc('WEEK', DATE_OF_PRICING) as week_start
FROM pricing_analytics.silver.daily_pricing_silver
LIMIT 10;
```

#### **Time Zone Operations**

```sql
-- Convert timestamp to different timezone
SELECT 
    current_timestamp() as utc_time,
    to_utc_timestamp(current_timestamp(), 'America/New_York') as ny_to_utc,
    from_utc_timestamp(current_timestamp(), 'Asia/Kolkata') as utc_to_ist,
    from_utc_timestamp(current_timestamp(), 'America/Los_Angeles') as utc_to_pst;
```

#### **Date Sequences**

```sql
-- Generate date sequence
SELECT explode(sequence(
    to_date('2023-01-01'),
    to_date('2023-12-31'),
    interval 1 day
)) as date;

-- Generate month sequence
SELECT explode(sequence(
    to_date('2023-01-01'),
    to_date('2023-12-31'),
    interval 1 month
)) as month_start;
```

#### **Practical Use Cases**

```sql
-- Find records from last trading day
SELECT *
FROM pricing_analytics.silver.daily_pricing_silver
WHERE DATE_OF_PRICING = (
    SELECT MAX(DATE_OF_PRICING) 
    FROM pricing_analytics.silver.daily_pricing_silver
);

-- Calculate age of data
SELECT 
    STATE_NAME,
    MARKET_NAME,
    DATE_OF_PRICING,
    datediff(current_date(), DATE_OF_PRICING) as days_old,
    CASE 
        WHEN datediff(current_date(), DATE_OF_PRICING) <= 7 THEN 'Recent'
        WHEN datediff(current_date(), DATE_OF_PRICING) <= 30 THEN 'This Month'
        WHEN datediff(current_date(), DATE_OF_PRICING) <= 90 THEN 'This Quarter'
        ELSE 'Older'
    END as data_age_category
FROM pricing_analytics.silver.daily_pricing_silver;

-- Business days calculation (excluding weekends)
SELECT 
    DATE_OF_PRICING,
    CASE 
        WHEN dayofweek(DATE_OF_PRICING) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type
FROM pricing_analytics.silver.daily_pricing_silver
WHERE dayofweek(DATE_OF_PRICING) NOT IN (1, 7);  -- Exclude weekends
```

**Date Format Patterns:**

| Pattern | Description | Example |
|---------|-------------|---------|
| `yyyy` | 4-digit year | 2023 |
| `yy` | 2-digit year | 23 |
| `MM` | 2-digit month | 10 |
| `M` | Month | 10 |
| `MMMM` | Month name | October |
| `MMM` | Short month name | Oct |
| `dd` | 2-digit day | 29 |
| `d` | Day | 29 |
| `EEEE` | Day name | Sunday |
| `EEE` | Short day name | Sun |
| `HH` | Hour (24-hour) | 14 |
| `hh` | Hour (12-hour) | 02 |
| `mm` | Minute | 30 |
| `ss` | Second | 45 |
| `a` | AM/PM | PM |

---

## 📊 Complete SQL Query Examples

### **Complex Analytical Query**

```sql
-- Comprehensive pricing analysis with multiple techniques
WITH monthly_pricing AS (
    -- Aggregate to monthly level
    SELECT 
        year(DATE_OF_PRICING) as year,
        month(DATE_OF_PRICING) as month,
        STATE_NAME,
        PRODUCT_NAME,
        AVG(MODAL_PRICE) as avg_price,
        MIN(MINIMUM_PRICE) as min_price,
        MAX(MAXIMUM_PRICE) as max_price,
        SUM(ARRIVAL_IN_TONNES) as total_arrival,
        COUNT(*) as trading_days
    FROM pricing_analytics.silver.daily_pricing_silver
    WHERE DATE_OF_PRICING >= '2023-01-01'
    GROUP BY year(DATE_OF_PRICING), month(DATE_OF_PRICING), STATE_NAME, PRODUCT_NAME
),
ranked_products AS (
    -- Rank products by price
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY year, month, STATE_NAME ORDER BY avg_price DESC) as price_rank,
        PERCENT_RANK() OVER (PARTITION BY year, month ORDER BY avg_price) as price_percentile
    FROM monthly_pricing
)
SELECT 
    year,
    month,
    STATE_NAME,
    PRODUCT_NAME,
    avg_price,
    min_price,
    max_price,
    total_arrival,
    trading_days,
    price_rank,
    ROUND(price_percentile * 100, 2) as price_percentile_pct,
    CASE 
        WHEN price_percentile >= 0.75 THEN 'High'
        WHEN price_percentile >= 0.25 THEN 'Medium'
        ELSE 'Low'
    END as price_category
FROM ranked_products
WHERE price_rank <= 10  -- Top 10 products per state per month
ORDER BY year, month, STATE_NAME, price_rank;
```

---

## 🚀 Performance Optimization

### **Query Optimization Tips**

```sql
-- 1. Use EXPLAIN to understand query plans
EXPLAIN SELECT * FROM pricing_analytics.silver.daily_pricing_silver WHERE STATE_NAME = 'Maharashtra';

-- 2. Analyze table for statistics
ANALYZE TABLE pricing_analytics.silver.daily_pricing_silver COMPUTE STATISTICS;

-- 3. Optimize and Z-Order for better performance
OPTIMIZE pricing_analytics.silver.daily_pricing_silver
ZORDER BY (STATE_NAME, PRODUCT_NAME);

-- 4. Use partition pruning
SELECT * 
FROM pricing_analytics.silver.daily_pricing_partitioned
WHERE STATE_NAME = 'Maharashtra'  -- Partition column
  AND year = 2023;                 -- Partition column

-- 5. Cache frequently used tables
CACHE TABLE pricing_analytics.silver.daily_pricing_silver;

-- 6. Uncache when done
UNCACHE TABLE pricing_analytics.silver.daily_pricing_silver;
```

---

## 📚 Additional Resources

- [Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-ref.html)
- [Delta Lake SQL Reference](https://docs.delta.io/latest/delta-batch.html)
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)
- [Date Functions Reference](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#date-and-timestamp-functions)

---

## ✅ Learning Checklist

After completing these notebooks, you should be able to:

- [ ] Create managed and external tables
- [ ] Understand table lifecycle management
- [ ] Perform date/time operations
- [ ] Write complex SQL queries
- [ ] Optimize tables for performance
- [ ] Work with partitioned tables
- [ ] Use window functions effectively
- [ ] Handle different file formats
- [ ] Implement best practices for production

---

**🔗 Next Steps:** Apply these Spark SQL concepts in the [Transformation](../02-Transformation/README.md) layer for building dimensional models.

---

**📧 Questions or Issues?** Check the main project [README](../README.md) for support information.
