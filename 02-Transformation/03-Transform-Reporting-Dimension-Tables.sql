-- Databricks notebook source
-- MAGIC %md
-- MAGIC | SOURCE_TABLE_NAME | SOURCE_COLUMN_NAME | REPORTING_TABLE_NAME | REPORTING_COLUMN_NAME | TRANSFORMATION RULE | CONDITIONS |
-- MAGIC | --- | --- |--- | --- |--- |--- |
-- MAGIC | silver.daily_pricing_silver	| state_name	|***REPORTING_DIM_STATE_GOLD***	|***STATE_NAME***	| Select Unique state_name Values | Identify New/Changed Records From the Source Table . Use ***lakehouse_updated_date Column*** in source table to Identify New/Changed Records|
-- MAGIC | DERIVED	| DERIVED	|***REPORTING_DIM_STATE_GOLD***	| ***STATE_ID***	| Generate Running Sequnece ID For Each Unique state_name Values | 1. Make Sure No Duplicate State_Name Values Loaded 2.  When loading new State_Name values in subsquent run , STATE_ID values need to be generated on top of existing MAX(STATE_ID)   |
-- MAGIC | DERIVED	| DERIVED	|***REPORTING_DIM_STATE_GOLD***	| ***lakehouse_inserted_date***	| Load current_timestamp() | |
-- MAGIC | DERIVED	| DERIVED	|***REPORTING_DIM_STATE_GOLD***	| ***lakehouse_updated_date***	| Load current_timestamp() | |

-- COMMAND ----------

USE CATALOG pricing_analytics;

CREATE OR REPLACE TABLE silver.reporting_dim_state_stage_1 AS
SELECT 
 DISTINCT STATE_NAME
FROM silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoad' AND process_status = 'Completed' )


-- COMMAND ----------

-- Create or replace the staging table for new state dimension records
CREATE OR REPLACE TABLE silver.reporting_dim_state_stage_2 AS 
SELECT 
  silverDim.STATE_NAME -- State name from the new data
 ,ROW_NUMBER() OVER (ORDER BY silverDim.STATE_NAME) as STATE_ID -- Assign a sequential surrogate key to each new state
 ,current_timestamp() as lakehouse_inserted_date -- Timestamp for when the record is inserted
 ,current_timestamp() as lakehouse_updated_date -- Timestamp for when the record is updated
FROM silver.reporting_dim_state_stage_1 silverDim
LEFT OUTER JOIN gold.reporting_dim_state_gold goldDim
  ON silverDim.STATE_NAME = goldDim.STATE_NAME -- Join to check if the state already exists in the gold dimension table
WHERE goldDim.STATE_NAME IS NULL -- Only include states that do not already exist in the gold dimension table

-- COMMAND ----------

-- Create or replace the staging table for new state dimension records with updated surrogate keys
CREATE OR REPLACE TABLE silver.reporting_dim_state_stage_3 AS
SELECT
  silverDim.STATE_NAME -- State name from the new data
  ,silverDim.STATE_ID + PREV_MAX_SK_ID as STATE_ID -- Assign a unique surrogate key by offsetting with the current max STATE_ID in gold
  ,current_timestamp() as lakehouse_inserted_date -- Timestamp for when the record is inserted
  ,current_timestamp() as lakehouse_updated_date -- Timestamp for when the record is updated
FROM 
  silver.reporting_dim_state_stage_2 silverDim
  CROSS JOIN (SELECT NVL(MAX(STATE_ID),0) as PREV_MAX_SK_ID FROM gold.reporting_dim_state_gold ) goldDim -- Get the current max STATE_ID from the gold table to offset new IDs

-- COMMAND ----------

INSERT INTO gold.reporting_dim_state_gold
SELECT
STATE_NAME
,STATE_ID
,current_timestamp()
,current_timestamp() 
From silver.reporting_dim_state_stage_3

-- COMMAND ----------

USE CATALOG pricing_analytics;
CREATE OR REPLACE TABLE silver.reporting_dim_market_stage_1 AS
SELECT 
 DISTINCT MARKET_NAME
FROM silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoad' AND process_status = 'Completed' )

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_market_stage_2 AS 
SELECT 
  silverDim.MARKET_NAME
 ,ROW_NUMBER() OVER (  ORDER BY silverDim.MARKET_NAME)  as MARKET_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM silver.reporting_dim_market_stage_1 silverDim
LEFT OUTER JOIN gold.reporting_dim_market_gold goldDim
ON silverDim.MARKET_NAME = goldDim.MARKET_NAME
WHERE goldDim.MARKET_NAME IS NULL;


-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_market_stage_3 AS 
SELECT
silverDim.MARKET_NAME 
,silverDim.MARKET_ID + PREV_MAX_SK_ID as MARKET_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
silver.reporting_dim_market_stage_2 silverDim
CROSS JOIN (SELECT NVL(MAX(MARKET_ID),0) as PREV_MAX_SK_ID FROM gold.reporting_dim_market_gold ) goldDim;

-- COMMAND ----------

INSERT INTO gold.reporting_dim_market_gold
SELECT
MARKET_NAME
,MARKET_ID
,current_timestamp() 
,current_timestamp() 
FROM silver.reporting_dim_market_stage_3;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_variety_stage_1 AS
SELECT 
 DISTINCT VARIETY
FROM silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoad' AND process_status = 'Completed' );


-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_variety_stage_2 AS 
SELECT 
  silverDim.VARIETY
 ,ROW_NUMBER() OVER (  ORDER BY silverDim.VARIETY)  as VARIETY_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM silver.reporting_dim_variety_stage_1 silverDim
LEFT OUTER JOIN gold.reporting_dim_variety_gold goldDim
ON silverDim.VARIETY= goldDim.VARIETY
WHERE goldDim.VARIETY IS NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_variety_stage_3 AS 
SELECT
silverDim.VARIETY 
,silverDim.VARIETY_ID + PREV_MAX_SK_ID as VARIETY_ID
,PREV_MAX_SK_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
silver.reporting_dim_variety_stage_2 silverDim
CROSS JOIN (SELECT nvl(MAX(VARIETY_ID),0) as PREV_MAX_SK_ID FROM gold.reporting_dim_variety_gold ) goldDim;



-- COMMAND ----------

INSERT INTO gold.reporting_dim_variety_gold
SELECT
VARIETY
,VARIETY_ID
,current_timestamp() 
,current_timestamp() 
FROM silver.reporting_dim_variety_stage_3

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_product_stage_1 AS
SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoad' AND process_status = 'Completed' );

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_product_stage_2 AS 
SELECT 
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
 ,ROW_NUMBER() OVER (  ORDER BY silverDim.PRODUCT_NAME,silverDim.PRODUCTGROUP_NAME)  as PRODUCT_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM silver.reporting_dim_product_stage_1 silverDim
LEFT OUTER JOIN gold.reporting_dim_product_gold goldDim
ON silverDim.PRODUCT_NAME= goldDim.PRODUCT_NAME
AND silverDim.PRODUCTGROUP_NAME = goldDim.PRODUCTGROUP_NAME
WHERE goldDim.PRODUCT_NAME IS NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_product_stage_3 AS 
SELECT
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
,silverDim.PRODUCT_ID + PREV_MAX_SK_ID as PRODUCT_ID
,PREV_MAX_SK_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
silver.reporting_dim_product_stage_2 silverDim
CROSS JOIN (SELECT nvl(MAX(PRODUCT_ID),0) as PREV_MAX_SK_ID FROM gold.reporting_dim_product_gold ) goldDim;

-- COMMAND ----------

INSERT INTO gold.reporting_dim_product_gold
SELECT
 PRODUCTGROUP_NAME
,PRODUCT_NAME
,PRODUCT_ID
,current_timestamp() 
,current_timestamp() 
FROM silver.reporting_dim_product_stage_3

-- COMMAND ----------

INSERT INTO  processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingDimensionTablesLoad' , max(lakehouse_updated_date) ,'Completed' FROM silver.daily_pricing_silver

-- COMMAND ----------

