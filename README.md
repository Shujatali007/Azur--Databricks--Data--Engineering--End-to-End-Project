# Azure Databricks Data Engineering - End-to-End Project

![Azure](https://img.shields.io/badge/Azure-0089D6?style=flat&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logoColor=white)

**End-to-end retail pricing analytics solution on Azure Databricks with Medallion Architecture. Ingests data from HTTP, Azure SQL, and APIs; transforms using Delta Lake & PySpark; builds star schema for BI with SCD Type 1 & 2 support. Features automated jobs, incremental loads, and Unity Catalog governance.**

## 📋 Project Overview

This project demonstrates a complete **end-to-end data engineering solution** built on **Azure Databricks**, implementing a **Medallion Architecture** (Bronze → Silver → Gold) for retail pricing analytics. The solution ingests data from multiple sources (HTTP endpoints, Azure SQL Database, REST APIs), transforms it using Delta Lake, and creates a star schema dimensional model for business intelligence and analytics.

### 🎯 Key Features

- **Multi-Source Data Ingestion**: HTTP endpoints, Azure SQL Database, REST APIs (Geocoding, Weather)
- **Medallion Architecture**: Bronze (raw), Silver (cleansed), Gold (analytics-ready) layers
- **Delta Lake Technology**: ACID transactions, time travel, schema evolution
- **Incremental Load Processing**: Process logs for tracking and incremental updates
- **Dimensional Modeling**: Star schema with slowly changing dimensions (SCD Type 1 & 2)
- **PySpark & Spark SQL**: Mixed workloads for optimal performance
- **Azure Data Lake Storage Gen2**: Scalable cloud storage integration

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├─────────────────────────────────────────────────────────────────┤
│  HTTP CSV Files  │  Azure SQL DB  │  Geocoding API  │  Weather API│
└──────────┬──────────────┬──────────────┬───────────────┬─────────┘
           │              │              │               │
           └──────────────┴──────────────┴───────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data)                       │
│              Azure Data Lake Storage Gen2 (ADLS)                 │
│   - daily-pricing/    - reference-data/                         │
│   - geo-location/     - weather-data/                           │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                SILVER LAYER (Cleansed & Conformed)               │
│                    Delta Lake Tables                             │
│   - daily_pricing_silver                                        │
│   - staging tables for dimension processing                     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              GOLD LAYER (Analytics-Ready / Star Schema)          │
│                    Delta Lake Tables                             │
│  Dimensions:                    │  Facts:                        │
│  - reporting_dim_date_gold      │  - reporting_fact_daily_       │
│  - reporting_dim_state_gold     │    pricing_gold                │
│  - reporting_dim_market_gold    │                                │
│  - reporting_dim_product_gold   │                                │
│  - reporting_dim_variety_gold   │                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📂 Project Structure

```
End-to-End DE Projects/
│
├── 01-Ingestion/                                    # Data Ingestion Layer
│   ├── 01-Deltahouse-Pre-Setup.sql                 # Initial catalog & schema setup
│   ├── 01-Ingest-Daily-Pricing-HTTP-Source-Data.py # Daily pricing CSV ingestion
│   ├── 03-Ingest-Pricing-Reference-DB-Source-Data.py # SQL DB reference data ingestion
│   ├── 04-Ingest-GeoLocation-API-Source-Data.py    # Geocoding API ingestion
│   ├── 05-Ingest-WeatherData-API-Source-Data.py    # Weather API ingestion
│   └── manifest.mf                                  # Manifest metadata
│
├── 02-Transformation/                               # Data Transformation Layer
│   ├── 01-Transform-Daily-Pricing-CSV-to-DELTA-Table.sql
│   ├── 03-deltalakehouse-silverlayer-table-setup.sql # Silver layer table definitions
│   ├── 03-Transform-Reporting-Date-Dimension-Table.py # Date dimension generator
│   ├── 03-Transform-Reporting-Dimension-Tables.sql # Dimension SCD Type 1
│   ├── 03-Transform-Reporting-Dimension-Tables-SCD-TYPE1.sql
│   ├── 03-Transform-Reporting-Dimension-Tables-SCD-TYPE2.sql
│   ├── 03-Transform-Reporting-Fact-Table.sql       # Fact table population
│   ├── 04-Deltalakehouse-gold-layer-reporting-tables-setup.py
│   ├── 04-Transform-DataLake-Geocoding.py          # Geocoding transformations
│   ├── 05-Transform-DataLake-WeatherData.py        # Weather data transformations
│   └── 06-Publish-DataLake-Price-Prediction.sql    # ML price prediction publishing
│
├── Delta Live Table/                                # DLT Pipeline Configurations
│   └── Delta_Live_table_pipeline-To-ADLS-Connectivity.txt
│
├── Delta-ELT-Pipeline/                              # DLT Pipeline Code
│   ├── README.md
│   ├── explorations/                                # Ad-hoc analysis notebooks
│   ├── transformations/                             # DLT dataset definitions
│   └── utilities/                                   # Helper functions
│       └── utils.py
│
├── Pyspark/                                         # PySpark Tutorials & Examples
│   ├── 01-Notebook-Introduction.py
│   ├── 03-PySpark-Introduction.py
│   ├── 04-Spark-DataFrame-Reader-and-Writer.py
│   ├── 05-Spark-DataFrame-Transformations-and-Actions.py
│   ├── 06-Spark-DataFrame-Additional-Transformations.py
│   ├── 07-Spark-DataFrame-DateTime-Functions.py
│   └── 08-Databricks-Utilities(dbutils)-Overview.py
│
├── Spark/                                           # Spark SQL Tutorials
│   ├── 01-Notebook-Introduction.py
│   ├── 02-Spark-SQL-Managed-Tables.sql
│   ├── 03-Spark-SQL-External-Tables.sql
│   └── 05-Spark-SQL-DateTime-Functions.sql
│
├── Spark Structured Streaming/                      # Streaming Examples
│   ├── 01-Ingest-Daily-Pricing-Streaming-Source-Data.sql
│   ├── 01-Processing-Streaming-Source-Data.py
│   ├── Spark-Structured-Streaming-Introduction.txt
│   └── Streaming-Reader-Writer-Final.txt
│
└── Tutorials/                                       # Learning Resources
    ├── Databricks_Bootcamp/                         # Comprehensive tutorials
    │   ├── Autoloader/
    │   ├── bronze_layer/
    │   ├── Data Access Control/
    │   ├── DATABRICKS SQL/
    │   ├── DELTA Optimization/
    │   ├── DELTALIVETABLES/
    │   ├── Lakeflow Jobs/
    │   ├── silver_layer/
    │   └── Unity Catalog Functions/
    └── RESOURCES/
```

---

## 📸 Project Screenshots

> **Note**: Screenshots are available in the `assets` folder. Images will be displayed once the repository is pushed to GitHub.

### Databricks Jobs & Pipelines Overview
![Jobs and Pipelines](assets/jobs-pipelines-overview.png)
*Overview of automated data engineering jobs and Delta Live Table pipeline*

**Key Highlights:**
- ✅ Delta-ELT-Pipeline (Pipeline) - Successfully running
- ✅ Job-Ingest-Daily-Pricing-HTTP-Source-Data - Scheduled every 12 hours
- ✅ Job-Ingest-pricing-reference-DB-Source-Data - Multi-task ingestion from Azure SQL
- ✅ Job-Transform-and-Load-Reporting-Tables - Scheduled daily
- ✅ Job-Transform-Daily-Pricing-CSV-to-DELTA-Table - Scheduled at 11:00 PM UTC

### Delta Live Table Pipeline
![Delta ELT Pipeline](assets/delta-elt-pipeline.png)
*Delta Live Table pipeline with streaming data transformations showing completed runs*

**Pipeline Features:**
- Streaming data ingestion and transformation
- Multiple staging tables for incremental processing
- Real-time data quality monitoring
- Automated pipeline orchestration

### Data Ingestion Jobs

#### Daily Pricing HTTP Source Ingestion
![Daily Pricing Ingestion](assets/job-ingest-daily-pricing.png)
*Scheduled job for ingesting daily pricing data from HTTP source (Every 12 hours)*

**Job Details:**
- **Schedule**: Every 12 hours
- **Tasks**: 1 upstream table, 1 downstream table
- **Duration**: ~3-7 minutes per run
- **Source**: HTTP endpoint (retailpricing.blob.core.windows.net)

#### Reference Data Ingestion from Azure SQL Database
![Reference Data Ingestion](assets/job-ingest-reference-data.png)
*Job for ingesting master data from Azure SQL Database (5 tasks)*

**Job Details:**
- **Tasks**: 5 parallel ingestion tasks
  - masterdata_market_address
  - masterdata_global_item_codes
  - masterdata_exchange_rates
  - masterdata_domestic_product_codes
  - masterdata_country_profile
- **Duration**: ~4-7 minutes per run
- **Source**: Azure SQL Database (asqludacoursesserver)

### Data Transformation Jobs

#### Transform and Load Reporting Tables
![Transform Reporting Tables](assets/job-transform-reporting-tables.png)
*Job for loading dimension and fact tables into Gold layer (20 upstream tables, 18 downstream tables)*

**Job Details:**
- **Schedule**: Every day (Paused - On demand)
- **Tasks**: 2 main tasks
  - reportingDimensionTablesLoad
  - reportingFactTableLoad
- **Lineage**: 20 upstream tables → 18 downstream tables
- **Duration**: ~5-10 minutes per run

#### Transform Daily Pricing to Delta Table
![Transform Daily Pricing](assets/job-transform-daily-pricing.png)
*Job for transforming CSV data to Delta table format (Scheduled at 11:00 PM UTC)*

**Job Details:**
- **Schedule**: Daily at 11:00 PM (UTC-00:00)
- **Task**: Daily_Pricing_Silver transformation
- **Lineage**: 3 upstream tables, 2 downstream tables
- **Duration**: ~4-8 minutes per run
- **Target**: pricing_analytics.silver.daily_pricing_silver

### Unity Catalog & Data Tables

#### Bronze Layer - Daily Pricing Raw Data
![Bronze Daily Pricing Table](assets/catalog-bronze-daily-pricing.png)
*Bronze layer table showing raw daily pricing data with schema and sample records*

**Table Details:**
- **Catalog**: pricing_analytics
- **Schema**: bronze
- **Table**: daily_pricing
- **Type**: External
- **Data Source**: CSV files from ADLS Gen2
- **Key Columns**: DATE_OF_PRICING, ROW_ID, STATE_NAME, MARKET_NAME, PRODUCT_NAME, VARIETY, ARRIVAL_IN_TONNES, MINIMUM_PRICE, MAXIMUM_PRICE, MODAL_PRICE

#### Silver Layer - Daily Pricing Cleansed Data
![Silver Daily Pricing Table](assets/catalog-silver-daily-pricing.png)
*Silver layer table with cleansed and typed pricing data across various states and markets*

**Table Details:**
- **Catalog**: pricing_analytics
- **Schema**: silver
- **Table**: daily_pricing_silver
- **Sample Insights**: 
  - Multi-state pricing data (Uttar Pradesh, Odisha, Jammu and Kashmir, Punjab, Karnataka, Kerala, Haryana, Tripura, Jharkhand)
  - Multiple markets (Najibabad, Sahiyapur, Ujhani, Rudrapur, Manjeswaram, Mottagaon, Akhnoor, etc.)
  - Diverse product groups (Fruits, Drug and Narcotics, Forest Products, Flowers, Oils and Fats)
  - Various products (Raddish, Ridge gourd, Round gourd, Snake gourd, Tobacco, Firewood, Wood, etc.)

#### Process Run Logs Table
![Process Run Logs](assets/catalog-processrunlogs.png)
*Process run logs tracking incremental load execution for all data pipelines*

**Table Details:**
- **Catalog**: pricing_analytics
- **Schema**: processrunlogs
- **Table**: deltalakehouse_process_runs
- **Purpose**: Track pipeline execution history and enable incremental processing
- **Key Processes Tracked**:
  - reportingDimensionTablesLoad
  - reportingFactTableLoad
  - daily_pricing_silver
  - dailyPricingSourceIngest

#### Gold Layer - Price Prediction Analytics
![Gold Price Prediction Table](assets/catalog-gold-price-prediction.png)
*Gold layer table containing ML-based price predictions for various products and markets*

**Table Details:**
- **Catalog**: pricing_analytics
- **Schema**: gold
- **Table**: datalake_price_prediction_gold
- **Sample Insights**:
  - Price predictions by date, state, and market
  - Product categories: Oil Seeds, Spices, Cereals
  - Products: Little gourd (Kundru), Mint(Pudina), Garlic, Jowar(Sorghum), Guar
  - Markets: Pune (Maharashtra)
  - Supports data-driven pricing strategy development

---

## 🔧 Technologies & Tools
```

---

## � Project Screenshots

### Databricks Jobs & Pipelines Overview
![Jobs and Pipelines](assets/jobs-pipelines-overview.png)
*Overview of automated data engineering jobs and Delta Live Table pipeline*

### Delta Live Table Pipeline
![Delta ELT Pipeline](assets/delta-elt-pipeline.png)
*Delta Live Table pipeline with streaming data transformations showing completed runs*

### Data Ingestion Jobs

#### Daily Pricing HTTP Source Ingestion
![Daily Pricing Ingestion](assets/job-ingest-daily-pricing.png)
*Scheduled job for ingesting daily pricing data from HTTP source (Every 12 hours)*

#### Reference Data Ingestion from Azure SQL Database
![Reference Data Ingestion](assets/job-ingest-reference-data.png)
*Job for ingesting master data from Azure SQL Database (5 tasks)*

### Data Transformation Jobs

#### Transform and Load Reporting Tables
![Transform Reporting Tables](assets/job-transform-reporting-tables.png)
*Job for loading dimension and fact tables into Gold layer (20 upstream tables, 18 downstream tables)*

#### Transform Daily Pricing to Delta Table
![Transform Daily Pricing](assets/job-transform-daily-pricing.png)
*Job for transforming CSV data to Delta table format (Scheduled at 11:00 PM UTC)*

---

## �🔧 Technologies & Tools

### Cloud & Platform
- **Microsoft Azure**: Cloud infrastructure
- **Azure Databricks**: Unified analytics platform
- **Azure Data Lake Storage Gen2 (ADLS)**: Scalable data lake storage
- **Azure SQL Database**: Relational data source

### Data Processing
- **Apache Spark**: Distributed data processing engine
- **PySpark**: Python API for Spark
- **Spark SQL**: SQL interface for structured data
- **Delta Lake**: ACID-compliant storage layer
- **Delta Live Tables (DLT)**: Declarative ETL framework

### Programming & Libraries
- **Python 3.x**: Primary programming language
- **Pandas**: Data manipulation library
- **Requests**: HTTP library for API calls
- **SQL**: Data querying and transformation

### APIs & Data Sources
- **Open-Meteo Geocoding API**: Location data enrichment
- **Weather API**: Weather data integration
- **HTTP/REST APIs**: External data sources

---

## 📊 Data Model

### Star Schema Design

#### **Dimension Tables**

| Table Name | Description | Key Attributes | SCD Type |
|------------|-------------|----------------|----------|
| `reporting_dim_date_gold` | Date dimension | DATE_ID, CALENDAR_DATE | N/A |
| `reporting_dim_state_gold` | State master data | STATE_ID, STATE_NAME | Type 1 |
| `reporting_dim_market_gold` | Market locations | MARKET_ID, MARKET_NAME | Type 1 |
| `reporting_dim_product_gold` | Product catalog | PRODUCT_ID, PRODUCT_NAME, PRODUCTGROUP_NAME | Type 2 |
| `reporting_dim_variety_gold` | Product varieties | VARIETY_ID, VARIETY | Type 1 |

#### **Fact Table**

| Table Name | Description | Measures |
|------------|-------------|----------|
| `reporting_fact_daily_pricing_gold` | Daily pricing facts | ARRIVAL_IN_TONNES, MINIMUM_PRICE, MAXIMUM_PRICE, MODAL_PRICE |

**Foreign Keys**: DATE_ID, STATE_ID, MARKET_ID, PRODUCT_ID, VARIETY_ID

---

## 🚀 Getting Started

### Prerequisites

1. **Azure Subscription** with access to:
   - Azure Databricks workspace
   - Azure Data Lake Storage Gen2
   - Azure SQL Database

2. **Databricks Cluster** with:
   - Databricks Runtime 11.3 LTS or higher
   - Python 3.9+
   - Access to Unity Catalog (recommended)

3. **Required Libraries**:
   - `pandas`
   - `requests`
   - Standard PySpark libraries (pre-installed)

### Setup Instructions

#### 1. **Configure Azure Resources**

```bash
# Create Resource Group
az group create --name rg-databricks-de --location eastus

# Create ADLS Gen2 Storage Account
az storage account create \
  --name adlsujadatalakehousedev \
  --resource-group rg-databricks-de \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create containers
az storage container create --name bronze --account-name adlsujadatalakehousedev
az storage container create --name silver --account-name adlsujadatalakehousedev
az storage container create --name gold --account-name adlsujadatalakehousedev
```

#### 2. **Configure Databricks Workspace**

- Create a **Unity Catalog** (if not already available)
- Set up **Service Principal** or **Access Connector** for ADLS Gen2 authentication
- Configure **external locations** for Bronze, Silver, Gold containers

#### 3. **Initialize Database Schema**

```sql
-- Run 01-Deltahouse-Pre-Setup.sql
USE CATALOG pricing_analytics;
CREATE SCHEMA IF NOT EXISTS processrunlogs;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

#### 4. **Configure Connection Strings**

Update the following in your notebooks:

```python
# ADLS Gen2 Storage Account
daiilyPricingSinkStorageAccountName = 'your-storage-account-name'

# Azure SQL Database
JDBCconnectionUrl = "jdbc:sqlserver://your-server.database.windows.net;encrypt=true;databaseName=your-db;user=your-user;password=your-password"
```

#### 5. **Run Ingestion Pipelines**

Execute notebooks in order:
1. `01-Ingest-Daily-Pricing-HTTP-Source-Data.py`
2. `03-Ingest-Pricing-Reference-DB-Source-Data.py`
3. `04-Ingest-GeoLocation-API-Source-Data.py`
4. `05-Ingest-WeatherData-API-Source-Data.py`

#### 6. **Run Transformation Pipelines**

Execute notebooks in order:
1. `01-Transform-Daily-Pricing-CSV-to-DELTA-Table.sql`
2. `03-Transform-Reporting-Date-Dimension-Table.py`
3. `03-Transform-Reporting-Dimension-Tables.sql`
4. `03-Transform-Reporting-Fact-Table.sql`

---

## 📖 Key Concepts Implemented

### 1. **Medallion Architecture**

- **Bronze Layer**: Raw data ingestion with minimal transformation
- **Silver Layer**: Cleansed, validated, and conformed data
- **Gold Layer**: Business-level aggregates and dimensional models

### 2. **Incremental Processing**

All pipelines use a **process run logs** table to track execution and enable incremental loads:

```sql
SELECT NVL(MAX(PROCESSED_FILE_TABLE_DATE)+1,'2023-01-01') 
FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE PROCESS_NAME = 'daily_pricing_ingest' AND PROCESS_STATUS='Completed'
```

### 3. **Slowly Changing Dimensions (SCD)**

- **Type 1**: Overwrite (State, Market, Variety)
- **Type 2**: Historical tracking (Product) with `start_date`, `end_date`, `is_current` columns

### 4. **Delta Lake Features**

```sql
-- ACID transactions
INSERT INTO gold.reporting_fact_daily_pricing_gold
SELECT ...

-- Time travel
SELECT * FROM silver.daily_pricing_silver VERSION AS OF 10

-- Schema evolution
ALTER TABLE silver.daily_pricing_silver 
ADD COLUMNS (new_column STRING)
```

### 5. **Surrogate Key Generation**

```sql
-- Generate sequential IDs offset by existing max
SELECT
  silverDim.STATE_NAME,
  silverDim.STATE_ID + PREV_MAX_SK_ID as STATE_ID
FROM silver.reporting_dim_state_stage_2 silverDim
CROSS JOIN (
  SELECT NVL(MAX(STATE_ID),0) as PREV_MAX_SK_ID 
  FROM gold.reporting_dim_state_gold
) goldDim
```

---

## 🔄 Data Pipeline Flow

### Daily Pricing Ingestion Flow

```
1. Widget Parameter → prm_processName
2. Query Process Logs → Get next file date
3. Build Source URL → https://retailpricing.blob.core.windows.net/daily-pricing/PW_MW_DR_{date}.csv
4. Read CSV with Pandas → Convert to Spark DataFrame
5. Add metadata → source_file_load_date
6. Write to Bronze → ADLS Gen2 (CSV format)
7. Log Process Run → Insert into process logs
```

### Dimension Processing Flow

```
1. Identify New Records → lakehouse_updated_date > last process run
2. Stage 1 → Extract distinct values from source
3. Stage 2 → Filter out existing dimension records
4. Stage 3 → Generate surrogate keys (offset from max)
5. Insert into Gold → Append new dimension records
6. Update Process Logs → Mark as completed
```

### Fact Table Processing Flow

```
1. Identify New Records → lakehouse_updated_date > last process run
2. Lookup Dimensions → Join to get surrogate keys
3. Transform Measures → Type casting, calculations
4. Insert into Gold → Append fact records
5. Update Process Logs → Mark as completed
```

---

## 📈 Sample Use Cases & Queries

### 1. Daily Pricing Analysis by State

```sql
SELECT 
  d.CALENDAR_DATE,
  s.STATE_NAME,
  p.PRODUCT_NAME,
  AVG(f.MODAL_PRICE) as avg_price,
  SUM(f.ARRIVAL_IN_TONNES) as total_arrival
FROM gold.reporting_fact_daily_pricing_gold f
JOIN gold.reporting_dim_date_gold d ON f.DATE_ID = d.DATE_ID
JOIN gold.reporting_dim_state_gold s ON f.STATE_ID = s.STATE_ID
JOIN gold.reporting_dim_product_gold p ON f.PRODUCT_ID = p.PRODUCT_ID
WHERE d.CALENDAR_DATE BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY d.CALENDAR_DATE, s.STATE_NAME, p.PRODUCT_NAME
ORDER BY d.CALENDAR_DATE, avg_price DESC
```

### 2. Market Price Trends

```sql
SELECT 
  m.MARKET_NAME,
  v.VARIETY,
  MIN(f.MINIMUM_PRICE) as lowest_price,
  MAX(f.MAXIMUM_PRICE) as highest_price,
  AVG(f.MODAL_PRICE) as average_price
FROM gold.reporting_fact_daily_pricing_gold f
JOIN gold.reporting_dim_market_gold m ON f.MARKET_ID = m.MARKET_ID
JOIN gold.reporting_dim_variety_gold v ON f.VARIETY_ID = v.VARIETY_ID
GROUP BY m.MARKET_NAME, v.VARIETY
ORDER BY average_price DESC
```

### 3. Product Performance Analysis

```sql
WITH product_metrics AS (
  SELECT 
    p.PRODUCTGROUP_NAME,
    p.PRODUCT_NAME,
    SUM(f.ARRIVAL_IN_TONNES) as total_volume,
    AVG(f.MODAL_PRICE) as avg_price,
    COUNT(DISTINCT f.DATE_ID) as days_traded
  FROM gold.reporting_fact_daily_pricing_gold f
  JOIN gold.reporting_dim_product_gold p ON f.PRODUCT_ID = p.PRODUCT_ID
  WHERE p.end_date IS NULL -- Current products only
  GROUP BY p.PRODUCTGROUP_NAME, p.PRODUCT_NAME
)
SELECT 
  PRODUCTGROUP_NAME,
  PRODUCT_NAME,
  total_volume,
  avg_price,
  days_traded,
  total_volume * avg_price as estimated_value
FROM product_metrics
ORDER BY estimated_value DESC
LIMIT 20
```

---

## 🛠️ Advanced Features

### 1. Delta Live Tables (DLT)

This project includes DLT pipeline configurations for declarative ETL:

```python
# Example DLT transformation
@dlt.table(
    name="daily_pricing_cleaned",
    comment="Cleaned daily pricing data"
)
def daily_pricing_cleaned():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/mnt/bronze/daily-pricing/")
        .filter(col("MODAL_PRICE").isNotNull())
    )
```

### 2. Spark Structured Streaming

For real-time processing scenarios:

```python
# Stream processing example
streamDF = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("abfss://bronze@storage.dfs.core.windows.net/daily-pricing/")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/daily-pricing")
    .table("pricing_analytics.silver.daily_pricing_silver")
)
```

### 3. Data Quality Checks

Implement data quality validations:

```sql
-- Check for null values in critical columns
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN MODAL_PRICE IS NULL THEN 1 ELSE 0 END) as null_prices,
  SUM(CASE WHEN DATE_OF_PRICING IS NULL THEN 1 ELSE 0 END) as null_dates
FROM silver.daily_pricing_silver
WHERE lakehouse_inserted_date >= CURRENT_DATE
```

---

## 📚 Learning Resources

This project is based on the Udemy course:
**"Azure Databricks Data Engineering with Real-Time Project"**

### Additional Resources

- [Azure Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Azure Data Lake Storage Gen2](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)
- [Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/)

---

## 🎓 Skills Demonstrated

### Technical Skills
- ✅ Azure Databricks workspace administration
- ✅ PySpark DataFrame API
- ✅ Spark SQL optimization
- ✅ Delta Lake ACID transactions
- ✅ Medallion Architecture implementation
- ✅ Dimensional modeling (Star Schema)
- ✅ Slowly Changing Dimensions (SCD Type 1 & 2)
- ✅ Incremental data processing
- ✅ REST API integration
- ✅ Azure Data Lake Storage Gen2
- ✅ Unity Catalog data governance
- ✅ Delta Live Tables (DLT)
- ✅ Spark Structured Streaming

### Data Engineering Best Practices
- ✅ Separation of concerns (Ingestion → Transformation → Serving)
- ✅ Process logging and monitoring
- ✅ Idempotent pipeline design
- ✅ Schema evolution handling
- ✅ Data quality validation
- ✅ Surrogate key management
- ✅ Partitioning strategies

---

## 🔐 Security & Best Practices

### Authentication
- Use **Azure Service Principal** or **Managed Identity** for ADLS access
- Store credentials in **Azure Key Vault**
- Configure **Unity Catalog** for fine-grained access control

### Data Governance
- Implement **Unity Catalog** for centralized governance
- Use **table ACLs** for row/column-level security
- Enable **audit logging** for compliance

### Performance Optimization
- **Partition** large tables by date
- Use **Z-Ordering** for frequently queried columns
- Enable **Auto Optimize** on Delta tables
- Implement **caching** for dimension tables

```sql
-- Optimize Delta table
OPTIMIZE pricing_analytics.gold.reporting_fact_daily_pricing_gold
ZORDER BY (DATE_ID, STATE_ID, PRODUCT_ID)
```

---

## 🤝 Contributing

This is a learning project, but suggestions and improvements are welcome!

---

## 📝 License

This project is created for educational purposes based on the Udemy course materials.

---

##  Acknowledgments

- **Udemy Course**: Azure Databricks Data Engineering with Real-Time Project
- **Microsoft Azure**: Cloud platform and services
- **Databricks**: Unified analytics platform
- **Delta Lake Community**: Open-source contributors

---

**⭐ If you found this project helpful, please give it a star!**
