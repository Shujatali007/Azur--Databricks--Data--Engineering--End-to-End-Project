-- Databricks notebook source
SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

SHOW CREATE TABLE pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE1 


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 (
  PRODUCTGROUP_NAME STRING,
  PRODUCT_NAME STRING,
  PRODUCT_ID BIGINT,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  lakehouse_inserted_date TIMESTAMP,
  lakehouse_updated_date TIMESTAMP)
USING delta

-- COMMAND ----------

SELECT * FROM pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 WHERe PRODUCT_NAME='Onion'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC * Peek at existing SCD-2 rows for 'Onion' (if any).
-- MAGIC
-- MAGIC **Example (initial state in SCD-2):**
-- MAGIC | PRODUCT_ID | PRODUCT_NAME | PRODUCTGROUP_NAME | start_date          | end_date | … |
-- MAGIC | ---------- | ------------ | ----------------- | ------------------- | -------- | - |
-- MAGIC | 101        | Onion        | Vegetables        | 2025-10-10 08:00:00 | NULL     | … |
-- MAGIC

-- COMMAND ----------

UPDATE pricing_analytics.silver.daily_pricing_silver
SET PRODUCTGROUP_NAME='Oil Seeds ',
lakehouse_updated_date = current_timestamp()
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the source (Silver), 'Onion' moves from Vegetables → **Oil Seeds ** (note the extra space!).
-- MAGIC
-- MAGIC This triggers a Type-2 change later.
-- MAGIC
-- MAGIC Silver (after):
-- MAGIC
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | lakehouse_updated_date |
-- MAGIC | ------------ | ----------------- | ---------------------- |
-- MAGIC | Onion        | Oil Seeds ␠       | 2025-10-14 21:30:00 …  |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stage-1: Incremental pick-up (watermark filter)

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_1 AS
SELECT 
 DISTINCT PRODUCT_NAME
 ,PRODUCTGROUP_NAME
FROM pricing_analytics.silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoadScdType2' AND process_status = 'Completed' );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pull only changed rows since the last successful run (watermark).
-- MAGIC
-- MAGIC Produces a small set of candidate rows to evaluate for SCD-2 changes.
-- MAGIC
-- MAGIC Stage-1 (example):
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME |
-- MAGIC | ------------ | ----------------- |
-- MAGIC | Onion        | Oil Seeds ␠       |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stage-2: Compare Stage-1 with Gold (detect New vs Changed + draft new SKs)

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_2 AS 
SELECT 
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
  ,goldDim.PRODUCT_NAME AS GOLD_PRODUCT_NAME
  ,goldDim.PRODUCT_ID AS GOLD_PRODUCT_ID
,ROW_NUMBER() OVER (  ORDER BY silverDim.PRODUCT_NAME,silverDim.PRODUCTGROUP_NAME)  as PRODUCT_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM pricing_analytics.silver.reporting_dim_product_stage_1 silverDim
LEFT OUTER JOIN pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 goldDim
ON silverDim.PRODUCT_NAME= goldDim.PRODUCT_NAME
AND goldDim.end_date IS NULL
WHERE goldDim.PRODUCT_NAME IS NULL OR silverDim.PRODUCTGROUP_NAME <> goldDim.PRODUCTGROUP_NAME

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * LEFT JOIN Silver (stage-1) to Gold (SCD-2) by name.
-- MAGIC
-- MAGIC * Keep rows where:
-- MAGIC
-- MAGIC     There’s no existing record in Gold (→ New), or
-- MAGIC
-- MAGIC     The group changed vs the current open Gold row (→ Changed).
-- MAGIC
-- MAGIC * Assign a temporary PRODUCT_ID using ROW_NUMBER() (this will be shifted to a real new SK in Stage-3).
-- MAGIC
-- MAGIC **Suppose Gold has (open) Vegetables, Silver now has Oil Seeds:**
-- MAGIC Stage-2 result:
-- MAGIC
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | GOLD_PRODUCT_NAME | GOLD_PRODUCT_ID | PRODUCT_ID | … |
-- MAGIC | ------------ | ----------------- | ----------------- | --------------- | ---------- | - |
-- MAGIC | Onion        | Oil Seeds ␠       | Onion             | 101             | 1          | … |
-- MAGIC
-- MAGIC
-- MAGIC * GOLD_PRODUCT_ID = 101 (the old open row).
-- MAGIC
-- MAGIC * PRODUCT_ID = 1 is temporary (not final).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stage-3: Finalize SKs + label rows (New vs Changed)

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.silver.reporting_dim_product_stage_3 AS 
SELECT
  silverDim.PRODUCT_NAME
  ,silverDim.PRODUCTGROUP_NAME
  ,GOLD_PRODUCT_ID
 ,silverDim.PRODUCT_ID + PREV_MAX_SK_ID  as PRODUCT_ID
,CASE WHEN GOLD_PRODUCT_NAME IS NULL THEN 'New' Else 'Changed' End as RECORD_STATUS
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
pricing_analytics.silver.reporting_dim_product_stage_2 silverDim
CROSS JOIN (SELECT nvl(MAX(PRODUCT_ID),0) as PREV_MAX_SK_ID FROM  pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 ) goldDim;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Shift the temporary PRODUCT_ID by the current max SK in Gold to produce the next surrogate key(s).
-- MAGIC
-- MAGIC * Tag each row:
-- MAGIC
-- MAGIC   * New if no prior Gold record,
-- MAGIC   * Changed if there was a prior Gold record.
-- MAGIC
-- MAGIC * **Assume current max** PRODUCT_ID in **Gold is 120**:
-- MAGIC Stage-3 result:
-- MAGIC
-- MAGIC | PRODUCT_NAME | PRODUCTGROUP_NAME | GOLD_PRODUCT_ID | PRODUCT_ID | RECORD_STATUS |
-- MAGIC | ------------ | ----------------- | --------------- | ---------- | ------------- |
-- MAGIC | Onion        | Oil Seeds ␠       | 101             | 121        | Changed       |
-- MAGIC

-- COMMAND ----------

SELECT * FROM pricing_analytics.silver.reporting_dim_product_stage_3 
--WHERE RECORD_STATUS='Changed'

-- COMMAND ----------

MERGE INTO  pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 goldDim
USING pricing_analytics.silver.reporting_dim_product_stage_3 silverDim
ON goldDim.PRODUCT_ID = silverDim.GOLD_PRODUCT_ID
WHEN MATCHED THEN 
UPDATE SET goldDim.end_date=current_timestamp()
          ,goldDim.lakehouse_updated_date=current_timestamp()
WHEN NOT MATCHED  THEN
INSERT (PRODUCTGROUP_NAME,PRODUCT_NAME,PRODUCT_ID,start_date,end_date,lakehouse_inserted_date,lakehouse_updated_date)
VALUES (silverDim.PRODUCTGROUP_NAME,silverDim.PRODUCT_NAME,silverDim.PRODUCT_ID,current_timestamp(),NULL,current_timestamp(),current_timestamp())


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC */
-- MAGIC * WHEN MATCHED (i.e., GOLD_PRODUCT_ID exists) → close the old row by setting its end_date = now.
-- MAGIC
-- MAGIC * WHEN NOT MATCHED → insert new rows (this covers New products that didn’t exist before).
-- MAGIC
-- MAGIC * After MERGE (for Onion):
-- MAGIC
-- MAGIC | PRODUCT_ID | PRODUCT_NAME | PRODUCTGROUP_NAME | start_date          | end_date            |
-- MAGIC | ---------- | ------------ | ----------------- | ------------------- | ------------------- |
-- MAGIC | 101        | Onion        | Vegetables        | 2025-10-10 08:00:00 | 2025-10-14 21:32:00 |
-- MAGIC
-- MAGIC `Note: The MERGE does not insert the new “Changed” version yet, because it only inserts on NOT MATCHED, and changed rows do match (on GOLD_PRODUCT_ID). That’s why the next step exists.`

-- COMMAND ----------

INSERT INTO pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2 
SELECT
PRODUCTGROUP_NAME
,PRODUCT_NAME
,PRODUCT_ID
,current_timestamp()
,NULL
,current_timestamp()
,current_timestamp()
FROM pricing_analytics.silver.reporting_dim_product_stage_3
WHERE RECORD_STATUS ='Changed'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Adds the new current version for changed records with the new PRODUCT_ID (121 in our example).
-- MAGIC
-- MAGIC Gold (after insert):
-- MAGIC
-- MAGIC | PRODUCT_ID | PRODUCT_NAME | PRODUCTGROUP_NAME | start_date          | end_date            |
-- MAGIC | ---------- | ------------ | ----------------- | ------------------- | ------------------- |
-- MAGIC | 101        | Onion        | Vegetables        | 2025-10-10 08:00:00 | 2025-10-14 21:32:00 |
-- MAGIC | 121        | Onion        | Oil Seeds ␠       | 2025-10-14 21:32:00 | NULL                |
-- MAGIC
-- MAGIC
-- MAGIC * This is the classic SCD-2 pattern:
-- MAGIC Old row closed.
-- MAGIC New row open.

-- COMMAND ----------

SELECT * FROM  pricing_analytics.gold.reporting_dim_product_gold_SCDTYPE2
WHERE PRODUCT_NAME='Onion'

-- COMMAND ----------

INSERT INTO  pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingDimensionTablesLoadScdType2' , max(lakehouse_updated_date) ,'Completed' FROM pricing_analytics.silver.daily_pricing_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Writes a “Completed” record with the max updated timestamp from Silver, so next run only picks newer changes.
-- MAGIC
-- MAGIC Run log (example):
-- MAGIC | PROCESS_NAME                         | PROCESSED_TABLE_DATETIME | PROCESS_STATUS |
-- MAGIC | ------------------------------------ | ------------------------ | -------------- |
-- MAGIC | reportingDimensionTablesLoadScdType2 | 2025-10-14 21:32:30      | Completed      |
-- MAGIC