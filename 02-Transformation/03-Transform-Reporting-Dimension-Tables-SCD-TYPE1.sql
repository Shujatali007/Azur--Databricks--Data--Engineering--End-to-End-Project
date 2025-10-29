-- Databricks notebook source
-- MAGIC %md
-- MAGIC Gold (reporting_dim_product_gold_SCDTYPE1)
-- MAGIC | PRODUCT_ID | PRODUCT_NAME | PRODUCTGROUP_NAME | lakehouse_inserted_date | lakehouse_updated_date |
-- MAGIC | ---------- | ------------ | ----------------- | ----------------------- | ---------------------- |
-- MAGIC | 101        | Onion        | Oil Seeds     | 2025-09-01       12:00       | 2025-09-01    12:00         |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver (daily_pricing_silver)
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | lakehouse_updated_date |
-- MAGIC | ------------ | ----------------- | ---------------------- |
-- MAGIC | Onion        | Vegetables    | 2025-10-14 12:00       |

-- COMMAND ----------

SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM pricing_analytics.silver.daily_pricing_silver
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
 LIKE pricing_analytics.gold.reporting_dim_product_gold


-- COMMAND ----------

SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

UPDATE pricing_analytics.silver.daily_pricing_silver
SET PRODUCTGROUP_NAME='Vegetables',
lakehouse_updated_date = current_timestamp()
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver (daily_pricing_silver)
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | lakehouse_updated_date |
-- MAGIC | ------------ | ----------------- | ---------------------- |
-- MAGIC | Onion        | Vegetable     | 2025-10-14 12:00       |
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_1 AS
SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM pricing_analytics.silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoadScdType1' AND process_status = 'Completed' );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### What this does
-- MAGIC
-- MAGIC Builds **Stage_1** with only rows modified after the last “Completed” run time.
-- MAGIC
-- MAGIC Keeps the load incremental and small.
-- MAGIC Silver (after update)
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | lakehouse_updated_date |
-- MAGIC | ------------ | ----------------- | ---------------------- |
-- MAGIC | Onion        | **Vegetables**    | 2025-10-14 20:59       |
-- MAGIC

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_1 

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_2 AS 
SELECT 
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
  ,goldDim.PRODUCT_NAME AS GOLD_PRODUCT_NAME
 ,CASE WHEN goldDim.PRODUCT_NAME IS NULL
 THEN ROW_NUMBER() OVER (  ORDER BY silverDim.PRODUCT_NAME,silverDim.PRODUCTGROUP_NAME) 
 ELSE goldDim.PRODUCT_ID END as PRODUCT_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM pricing_analytics.silver.reporting_dim_product_stage_1 silverDim
LEFT OUTER JOIN pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 goldDim
ON silverDim.PRODUCT_NAME= goldDim.PRODUCT_NAME
WHERE goldDim.PRODUCT_NAME IS NULL OR silverDim.PRODUCTGROUP_NAME <> goldDim.PRODUCTGROUP_NAME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What this does
-- MAGIC
-- MAGIC **Left join** Stage_1 to Gold on PRODUCT_NAME.
-- MAGIC
-- MAGIC Filters to:
-- MAGIC   * **New products** (not in Gold), or
-- MAGIC
-- MAGIC   * **Existing products with changed attributes** (e.g., group changed).
-- MAGIC
-- MAGIC **PRODUCT_ID** rule:
-- MAGIC   * If exists in Gold → keep the **existing PRODUCT_ID** (SCD-1 rule: overwrite in place).
-- MAGIC   * If new → temporarily assign a **ROW_NUMBER()** (placeholder).
-- MAGIC
-- MAGIC ### Stage_2 (reporting_dim_product_stage_2)
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | GOLD_PRODUCT_NAME | PRODUCT_ID | lakehouse_inserted_date | lakehouse_updated_date |
-- MAGIC | ------------ | ----------------- | ----------------- | ---------- | ----------------------- | ---------------------- |
-- MAGIC | Onion        | Vegetables        | Onion             | **101**    | now                     | now                    |
-- MAGIC
-- MAGIC  Because Onion already existed in Gold, we retain **PRODUCT_ID=101**.
-- MAGIC
-- MAGIC `If it were a new product (say “Leek”), GOLD_PRODUCT_NAME would be NULL and PRODUCT_ID would be a temporary row_number like 1.`
-- MAGIC

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC silver.reporting_dim_state_stage_2:
-- MAGIC | STATE_NAME | STATE_ID |
-- MAGIC | ---------- | -------- |
-- MAGIC | California | 1        |
-- MAGIC | Nevada     | 2        |
-- MAGIC | Arizona    | 3        |
-- MAGIC
-- MAGIC
-- MAGIC Subquery (right side of CROSS JOIN):
-- MAGIC PREV_MAX_SK_ID
-- MAGIC | PREV_MAX_SK_ID |
-- MAGIC | -------------- |
-- MAGIC | 3              |
-- MAGIC
-- MAGIC CROSS JOIN Result:
-- MAGIC | STATE_NAME | STATE_ID | PREV_MAX_SK_ID |
-- MAGIC | ---------- | -------- | -------------- |
-- MAGIC | California | 1        | 3              |
-- MAGIC | Nevada     | 2        | 3              |
-- MAGIC | Arizona    | 3        | 3              |
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC | STATE_NAME | Final STATE_ID |
-- MAGIC | ---------- | -------------- |
-- MAGIC | California | 4              |
-- MAGIC | Nevada     | 5              |
-- MAGIC | Arizona    | 6              |
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **What this does**
-- MAGIC
-- MAGIC   For **new** products, converts the temporary ROW_NUMBER() to a **globally unique** key by **offsetting** with the current max PRODUCT_ID in Gold.
-- MAGIC
-- MAGIC   For **existing** products (like Onion), it **keeps the same PRODUCT_ID**.
-- MAGIC
-- MAGIC **Stage_3 (reporting_dim_product_stage_3)**
-- MAGIC
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | PRODUCT_ID | lakehouse_inserted_date | lakehouse_updated_date |
-- MAGIC | ------------ | ----------------- | ---------- | ----------------------- | ---------------------- |
-- MAGIC | Onion        | Vegetables        | **101**    | now                     | now                    |
-- MAGIC
-- MAGIC `If “Leek” was new and PREV_MAX_SK_ID = 120, a Stage_2 row_number 1 would become 121 here`
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_3 AS 
SELECT
  silverDim.PRODUCTGROUP_NAME
  ,silverDim.PRODUCT_NAME
,CASE WHEN GOLD_PRODUCT_NAME IS NULL
THEN silverDim.PRODUCT_ID + PREV_MAX_SK_ID 
ELSE PRODUCT_ID END as PRODUCT_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
pricing_analytics.silver.reporting_dim_product_stage_2 silverDim
CROSS JOIN (SELECT nvl(MAX(PRODUCT_ID),0) as PREV_MAX_SK_ID FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 ) goldDim;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_3

-- COMMAND ----------

MERGE INTO pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 goldDim
USING pricing_analytics.silver.reporting_dim_product_stage_3 silverDim
ON goldDim.PRODUCT_NAME = silverDim.PRODUCT_NAME
WHEN MATCHED THEN 
UPDATE SET goldDim.PRODUCTGROUP_NAME=silverDim.PRODUCTGROUP_NAME
           ,goldDim.lakehouse_updated_date=current_timestamp()
WHEN NOT MATCHED THEN
INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What this does (SCD-1 behavior)
-- MAGIC   * **MATCHED (existing product)**: Overwrite the changed attribute(s).
-- MAGIC     → Onion’s PRODUCTGROUP_NAME becomes Vegetables with the same `PRODUCT_ID=101`.
-- MAGIC   * **NOT MATCHED (new product)**: Insert a new row with a **new unique** `PRODUCT_ID`.
-- MAGIC Gold (after MERGE)
-- MAGIC
-- MAGIC | PRODUCT_ID | PRODUCT_NAME | PRODUCTGROUP_NAME | lakehouse_inserted_date | lakehouse_updated_date |
-- MAGIC | ---------- | ------------ | ----------------- | ----------------------- | ---------------------- |
-- MAGIC | 101        | Onion        | **Vegetables**    | 2025-09-01              | 2025-10-14 21:00       |
-- MAGIC
-- MAGIC `Note how history is not kept. Old value (“oil”) is overwritten, which is exactly SCD Type-1.`
-- MAGIC

-- COMMAND ----------

SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC (Implied) Log success time
-- MAGIC
-- MAGIC After a successful run, your orchestration should write a row into DELTALAKEHOUSE_PROCESS_RUNS like:
-- MAGIC | process_name                         | process_status | PROCESSED_TABLE_DATETIME |
-- MAGIC | ------------------------------------ | -------------- | ------------------------ |
-- MAGIC | reportingDimensionTablesLoadScdType1 | Completed      | 2025-10-14 21:00         |
-- MAGIC

-- COMMAND ----------

INSERT INTO  pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingDimensionTablesLoadScdType1' , max(lakehouse_updated_date) ,'Completed' FROM pricing_analytics.silver.daily_pricing_silver