-- Databricks notebook source
-- MAGIC %python
-- MAGIC dailyPricingStramingSourceFolderPath = "abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-source-data"

-- COMMAND ----------

USE CATALOG pricing_analytics;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dailyPricingSourceStreamDF = spark.sql("""SELECT
-- MAGIC current_timestamp() as DATETIME_OF_PRICING,
-- MAGIC cast(ROW_ID as bigint) ,
-- MAGIC STATE_NAME,
-- MAGIC MARKET_NAME,
-- MAGIC PRODUCTGROUP_NAME,
-- MAGIC PRODUCT_NAME,
-- MAGIC VARIETY,
-- MAGIC ORIGIN,
-- MAGIC cast(ARRIVAL_IN_TONNES as decimal(18,2)) + extract(seconds from current_timestamp() )/100 as ARRIVAL_IN_TONNES, 
-- MAGIC cast(MINIMUM_PRICE as decimal(36,2)) + extract(seconds from current_timestamp() )/100 as MINIMUM_PRICE,
-- MAGIC cast(MAXIMUM_PRICE as decimal(36,2)) + extract(seconds from current_timestamp() )/100 as MAXIMUM_PRICE,
-- MAGIC cast(MODAL_PRICE as decimal(36,2))+ extract(seconds from current_timestamp() )/100 as MODAL_PRICE,
-- MAGIC current_timestamp() as source_stream_load_datetime
-- MAGIC FROM bronze.daily_pricing
-- MAGIC WHERE DATE_OF_PRICING='01/01/2023'""")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC (dailyPricingSourceStreamDF
-- MAGIC .write
-- MAGIC .mode('append')
-- MAGIC .json(dailyPricingStramingSourceFolderPath)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(dailyPricingStramingSourceFolderPath)

-- COMMAND ----------

