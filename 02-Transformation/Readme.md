# 🔄 Data Transformation Layer

This folder contains all data transformation pipelines that process data from the **Bronze Layer** (raw data) through the **Silver Layer** (cleansed data) to the **Gold Layer** (analytics-ready dimensional model).

## 📋 Overview

The transformation layer implements:
- **Data Cleansing**: Type casting, null handling, data quality checks
- **Dimensional Modeling**: Star schema with fact and dimension tables
- **Slowly Changing Dimensions (SCD)**: Type 1 and Type 2 implementations
- **Incremental Processing**: Process logs for tracking and updates
- **Surrogate Key Generation**: Automated key assignment
- **Data Quality Validation**: Schema enforcement and business rule checks

---

## 📂 Files in This Folder

### 1️⃣ **01-Transform-Daily-Pricing-CSV-to-DELTA-Table.sql**
**Purpose**: Transform raw CSV pricing data from Bronze to Silver layer as Delta tables

**Transformation Type**: Bronze → Silver

**Key Operations:**
- ✅ Read CSV files from ADLS Gen2 Bronze layer
- ✅ Schema inference and type casting
- ✅ Data quality validation (remove nulls, validate ranges)
- ✅ Add metadata columns (lakehouse_inserted_date, lakehouse_updated_date)
- ✅ Write as Delta table with ACID properties
- ✅ Incremental append mode

**Data Flow:**
```
Bronze CSV Files → Data Validation → Type Casting → Silver Delta Table
                                                   ↓
                                        Update Process Logs
```

**Source:**
```sql
-- Bronze location
abfss://bronze@{storage_account}.dfs.core.windows.net/daily-pricing/
```

**Target:**
```sql
-- Silver Delta table
pricing_analytics.silver.daily_pricing_silver
```

**Schema Transformation:**
```sql
-- Input (CSV - all strings)
DATE_OF_PRICING: string
STATE_NAME: string
MARKET_NAME: string
ARRIVAL_IN_TONNES: string
MINIMUM_PRICE: string
MAXIMUM_PRICE: string
MODAL_PRICE: string

-- Output (Delta - typed)
DATE_OF_PRICING: date
STATE_NAME: string
MARKET_NAME: string
ARRIVAL_IN_TONNES: decimal(10,2)
MINIMUM_PRICE: decimal(10,2)
MAXIMUM_PRICE: decimal(10,2)
MODAL_PRICE: decimal(10,2)
lakehouse_inserted_date: timestamp
lakehouse_updated_date: timestamp
```

**Validation Rules:**
- Remove records with NULL modal_price
- Validate date formats
- Ensure numeric fields are valid
- Remove duplicate records

**Execution Frequency**: Daily (Scheduled at 11:00 PM UTC)

---

### 2️⃣ **03-deltalakehouse-silverlayer-table-setup.sql**
**Purpose**: Define and create staging tables in Silver layer for dimension processing

**Tables Created:**

#### **State Dimension Staging**
```sql
pricing_analytics.silver.reporting_dim_state_stage_1
pricing_analytics.silver.reporting_dim_state_stage_2
pricing_analytics.silver.reporting_dim_state_stage_3
```

#### **Market Dimension Staging**
```sql
pricing_analytics.silver.reporting_dim_market_stage_1
pricing_analytics.silver.reporting_dim_market_stage_2
pricing_analytics.silver.reporting_dim_market_stage_3
```

#### **Product Dimension Staging**
```sql
pricing_analytics.silver.reporting_dim_product_stage_1
pricing_analytics.silver.reporting_dim_product_stage_2
pricing_analytics.silver.reporting_dim_product_stage_3
```

#### **Variety Dimension Staging**
```sql
pricing_analytics.silver.reporting_dim_variety_stage_1
pricing_analytics.silver.reporting_dim_variety_stage_2
pricing_analytics.silver.reporting_dim_variety_stage_3
```

**Staging Table Purpose:**
- **Stage 1**: Extract distinct values from Silver source
- **Stage 2**: Filter out existing dimension records (identify new records)
- **Stage 3**: Generate surrogate keys for new records

**When to run**: Once during initial setup, or when adding new dimension staging tables

---

### 3️⃣ **03-Transform-Reporting-Date-Dimension-Table.py**
**Purpose**: Generate a complete date dimension table for time-based analytics

**Transformation Type**: Silver → Gold

**Key Features:**
- ✅ Generates dates for configurable range (e.g., 2020-2030)
- ✅ Calculates date attributes: year, month, quarter, day of week
- ✅ Adds fiscal calendar attributes
- ✅ Includes holiday flags (configurable)
- ✅ Weekend/weekday indicators
- ✅ Date formatting variations

**Generated Columns:**
```python
DATE_ID: int                    # Surrogate key (YYYYMMDD format)
CALENDAR_DATE: date             # Actual date
YEAR: int                       # Year (2
