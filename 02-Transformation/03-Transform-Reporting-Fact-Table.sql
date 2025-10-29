-- Databricks notebook source
-- MAGIC %md
-- MAGIC | SOURCE_TABLE_NAME | SOURCE_COLUMN_NAME | REPORTING_TABLE_NAME | REPORTING_COLUMN_NAME | TRANSFORMATION RULE | CONDITIONS |
-- MAGIC | --- | --- |--- | --- |--- |--- |
-- MAGIC | silver.daily_pricing_silver	| DATE_OF_PRICING	|reporting_fact_daily_pricing_gold| DATE_ID| Lookup Source DATE_OF_PRICING value against CALENDAR_DATE value on reporting_dim_date_gold table and select DATE_ID | Identify New/Changed Records From the Source |
-- MAGIC | silver.daily_pricing_silver	| STATE_NAME	|reporting_fact_daily_pricing_gold| STATE_ID| Lookup Source STATE_NAME value against STATE_NAME value on reporting_dim_state_gold table and select STATE_ID |  |
-- MAGIC | silver.daily_pricing_silver	| MARKET_NAME	|reporting_fact_daily_pricing_gold| MARKET_ID| Lookup Source MARKET_NAME value against MARKET_NAME value on reporting_dim_market_gold table and select MARKET_ID |  |
-- MAGIC | silver.daily_pricing_silver	| PRODUCTGROUP_NAME , PRODUCT_NAME	|reporting_fact_daily_pricing_gold| PRODUCT_ID| Lookup Source PRODUCTGROUP_NAME and PRODUCT_NAME values against PRODUCTGROUP_NAME and PRODUCT_NAME values on reporting_dim_prduct_gold table and select PRODUCT_ID |  |
-- MAGIC | silver.daily_pricing_silver	| VARIETY	|reporting_fact_daily_pricing_gold| VARIETY_ID| Lookup Source VARIETY value against VARIETY value on reporting_dim_variety_gold table and select VARIETY_ID |  |
-- MAGIC | silver.daily_pricing_silver	| ROW_ID	|reporting_fact_daily_pricing_gold| ROW_ID| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| ARRIVAL_IN_TONNES	|reporting_fact_daily_pricing_gold| ARRIVAL_IN_TONNES| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| MINIMUM_PRICE	|reporting_fact_daily_pricing_gold| MINIMUM_PRICE| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| MAXIMUM_PRICE	|reporting_fact_daily_pricing_gold| MAXIMUM_PRICE| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| MODAL_PRICE	|reporting_fact_daily_pricing_gold| MODAL_PRICE| Direct Mapping |  |
-- MAGIC | DERIVED	| DERIVED	|reporting_fact_daily_pricing_gold	| lakehouse_inserted_date	| Load current_timestamp() | |
-- MAGIC | DERIVED	| DERIVED	|reporting_fact_daily_pricing_gold	| lakehouse_updated_date	| Load current_timestamp() | |

-- COMMAND ----------

USE CATALOG pricing_analytics;
INSERT INTO gold.reporting_fact_daily_pricing_gold
SELECT
dateDim.DATE_ID
,stateDIM.STATE_ID
,marketDim.MARKET_ID
,productDim.PRODUCT_ID
,varieytyDim.VARIETY_ID
,silverFact.ROW_ID
,silverFact.ARRIVAL_IN_TONNES
,silverFact.MAXIMUM_PRICE
,silverFact.MINIMUM_PRICE
,silverFact.MODAL_PRICE
,current_timestamp()
,current_timestamp()
FROM silver.daily_pricing_silver silverFact
LEFT OUTER JOIN gold.reporting_dim_date_gold dateDim
on silverFact.DATE_OF_PRICING = dateDim.CALENDAR_DATE
LEFT OUTER JOIN gold.reporting_dim_state_gold stateDim
on silverFact.STATE_NAME = stateDim.STATE_NAME
LEFT OUTER JOIN gold.reporting_dim_market_gold marketDim
on silverFact.MARKET_NAME = marketDim.MARKET_NAME
LEFT OUTER JOIN gold.reporting_dim_product_gold productDim
on silverFact.PRODUCT_NAME = productDim.PRODUCT_NAME
AND silverFact.PRODUCTGROUP_NAME = productDim.PRODUCTGROUP_NAME
AND productDim.end_date IS NULL
LEFT OUTER JOIN gold.reporting_dim_variety_gold varieytyDim
on silverFact.VARIETY = varieytyDim.VARIETY
WHERE silverFact.lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingFactTableLoad' AND process_status = 'Completed' )

-- COMMAND ----------

INSERT INTO  processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingFactTableLoad' , max(lakehouse_updated_date) ,'Completed' FROM silver.daily_pricing_silver

-- COMMAND ----------

select * from gold.reporting_fact_daily_pricing_gold