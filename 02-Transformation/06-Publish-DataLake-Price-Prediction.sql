-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Description:
-- MAGIC Integrate Daily Pricing Data , Geolocation Data and Weather Data to Publish in Gold Layer as a source data for Future Price Prediction Using AI
-- MAGIC
-- MAGIC ##### Source Tables:
-- MAGIC pricing_analytics.silver.daily_pricing_silver , pricing_analytics.silver.geo_location_silver , pricing_analytics.silver.weather_data_silver
-- MAGIC
-- MAGIC ##### Target Table name : DataLake_Price-Prediction-Gold
-- MAGIC ###### Target Table Column Mappings:
-- MAGIC | SOURCE_TABLE_NAME | SOURCE_COLUMN_NAME | DATALAKE_TABLE_NAME | DATALAKE_COLUMN_NAME | TRANSFORMATION RULE | CONDITIONS |
-- MAGIC | --- | --- |--- | --- |--- |--- |
-- MAGIC | silver.daily_pricing_silver	| DATE_OF_PRICING	|datalake_price_prediction_gold| DATE_OF_PRICING| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| STATE_NAME	|datalake_price_prediction_gold| STATE_NAME| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| MARKET_NAME	|datalake_price_prediction_gold|  MARKET_NAME	| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| PRODUCTGROUP_NAME |datalake_price_prediction_gold| PRODUCT_ID| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| PRODUCT_NAME	|datalake_price_prediction_gold| PRODUCT_ID| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| VARIETY	|datalake_price_prediction_gold| VARIETY_ID|Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| ROW_ID	|datalake_price_prediction_gold| ROW_ID| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| ARRIVAL_IN_TONNES	|datalake_price_prediction_gold| ARRIVAL_IN_TONNES| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| MINIMUM_PRICE	|datalake_price_prediction_gold| MINIMUM_PRICE| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| MAXIMUM_PRICE	|datalake_price_prediction_gold| MAXIMUM_PRICE| Direct Mapping |  |
-- MAGIC | silver.daily_pricing_silver	| MODAL_PRICE	|datalake_price_prediction_gold| MODAL_PRICE| Direct Mapping |  |
-- MAGIC | silver.geo_location_silver 	| latitude	|datalake_price_prediction_gold| MARKET_LATITUDE| Change the Source Column Name | daily_pricing_silver.STATE_NAME = geo_location_silver.stateName AND daily_pricing_silver.MARKET_NAME = geo_location_silver.marketName AND geo_location_silver.countryName = 'India' |
-- MAGIC | silver.geo_location_silver 	| longitude	|datalake_price_prediction_gold| MARKET_LONGITUDE| Change the Source Column Name |  |
-- MAGIC | silver.geo_location_silver 	| population	|datalake_price_prediction_gold| MARKET_POPULATION| Change the Source Column Name  |  |
-- MAGIC | silver.weather_data_silver 	| unitOfTemparature	|datalake_price_prediction_gold| TEMPARATURE_UNIT| Change the Source Column Name  | daily_pricing_silver.MARKET_NAME = weather_data_silver.marketName AND daily_pricing_silver.DATE_OF_PRICING = weather_data_silver.weatherDate |
-- MAGIC | silver.weather_data_silver 	| maximumTemparature	|datalake_price_prediction_gold| MARKET_MAX_TEMPARATURE | Change the Source Column Name  |  |
-- MAGIC | silver.weather_data_silver 	| minimumTemparature	|datalake_price_prediction_gold| MARKET_MIN_TEMPARATURE | Change the Source Column Name  |  |
-- MAGIC | silver.weather_data_silver 	| unitOfRainFall	|datalake_price_prediction_gold| RAINFALL_UNIT| Change the Source Column Name  |  |
-- MAGIC | silver.weather_data_silver 	| rainFall	|datalake_price_prediction_gold| MARKET_DAILY_RAINFALL| Change the Source Column Name  |  |
-- MAGIC | DERIVED	| DERIVED	|datalake_price_prediction_gold	| lakehouse_inserted_date	| Load current_timestamp() | |
-- MAGIC | DERIVED	| DERIVED	|datalake_price_prediction_gold	| lakehouse_updated_date	| Load current_timestamp() | |
-- MAGIC
-- MAGIC
-- MAGIC - <a href="https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html" target="_blank">**MERGE TABLE** </a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 1: Create and Load Gold Layer Price Prediction Table
-- MAGIC
-- MAGIC 1. SELECT the source columns mentioned above from the source table pricing_analytics.silver.daily_pricing_silver
-- MAGIC 1. SELECT the source columns mentioned above from the source table pricing_analytics.silver.geo_location_silver and changes source column names to target column names as mentioned above mapping
-- MAGIC 1. Include JOIN conditions between pricing_analytics.silver.daily_pricing_silver table and pricing_analytics.silver.geo_location_silver Using the Join conditions mentioned in above mapping
-- MAGIC 1. SELECT the source columns mentioned above from the source table pricing_analytics.silver.weather_data_silver  and changes source column names to target column names as mentioned above mapping
-- MAGIC 1. Include JOIN conditions between pricing_analytics.silver.daily_pricing_silver table and pricing_analytics.silver.weather_data_silver  Using the Join conditions mentioned in above mapping
-- MAGIC 1. Map current_timestamp() function to additional new columns lakehouse_inserted_date and lakehouse_updated_date
-- MAGIC 1. CREATE the target table to store the output of SELECT statement to publish the transformed data.

-- COMMAND ----------

CREATE OR REPLACE TABLE pricing_analytics.gold.`DataLake_Price_Prediction_Gold` AS
SELECT DISTINCT
dailypricing.DATE_OF_PRICING
,dailypricing.ROW_ID
,dailypricing.STATE_NAME
,dailypricing.MARKET_NAME
,dailypricing.PRODUCTGROUP_NAME
,dailypricing.PRODUCT_NAME
,dailypricing.VARIETY
,dailypricing.ORIGIN
,dailypricing.ARRIVAL_IN_TONNES
,dailypricing.MINIMUM_PRICE
,dailypricing.MAXIMUM_PRICE
,dailypricing.MODAL_PRICE
,geolocation.latitude as MARKET_LATITUDE
,geolocation.longitude as MARKET_LONGITUDE
,geolocation.population as MARKET_POPULATION
,weatherdata.unitOfTemparature as TEMPARATURE_UNIT
,weatherdata.maximumTemparature as MARKET_MAX_TEMPARATURE
,weatherdata.minimumTemparature as MARKET_MIN_TEMPARATURE
,weatherdata.unitOfRainFall as RAINFALL_UNIT
,weatherdata.rainFall as MARKET_DAILY_RAINFALL
,current_timestamp as lakehouse_inserted_date
,current_timestamp as lakehouse_updated_date
FROM pricing_analytics.silver.daily_pricing_silver dailypricing
INNER JOIN pricing_analytics.silver.geo_location_silver geolocation
ON STATE_NAME = geolocation.stateName
AND MARKET_NAME = geolocation.marketName
AND countryName = 'India'
INNER JOIN pricing_analytics.silver.weather_data_silver weatherdata
ON weatherdata.marketName = MARKET_NAME
AND DATE_OF_PRICING = weatherdata.weatherDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 2: Test The Data Stored in Gold Layer Table And Highlight Any Data Quality Issues
-- MAGIC
-- MAGIC 1. Write SELECT query to select the data from pricing_analytics.gold.DataLake_Price-Prediction_Gold table
-- MAGIC 1. Check the data for any one of the Market Name and make sure there are no data quality issues
-- MAGIC 1. Raise any Data Quality Issuesin Target Table  as a Query in Udemy Course

-- COMMAND ----------

SELECT * FROM pricing_analytics.gold.DataLake_Price_Prediction_Gold
--WHERE MARKET_POPULATION IS NOT NULL

-- COMMAND ----------

