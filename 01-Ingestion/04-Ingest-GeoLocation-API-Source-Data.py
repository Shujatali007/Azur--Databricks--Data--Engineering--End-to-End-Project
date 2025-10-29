# Databricks notebook source
# MAGIC %md
# MAGIC source API URL : "https://geocoding-api.open-meteo.com/v1/search?name=kovilpatti&count=10&language=en&format=json"
# MAGIC
# MAGIC JSON Target File Path : "abfss://bronze@datalakestorageaccountname.dfs.core.windows.net/geo-location/"

# COMMAND ----------

geoLocationSourceAPIURL = "https://geocoding-api.open-meteo.com/v1/search?name=kovilpatti&count=10&language=en&format=json"

geoLocationSourceAPIBaseURL = "https://geocoding-api.open-meteo.com/v1/search?name="
geoLocationSourceAPIURLOptions = "&count=10&language=en&format=json"

geoLocationSinkLayerName = 'bronze'
geoLocationSinkStorageAccountName = 'adlsujadatalakehousedev'
geoLocationSinkFolderName = 'geo-location'

geoLocationSinkFolderPath = f"abfss://{geoLocationSinkLayerName}@{geoLocationSinkStorageAccountName}.dfs.core.windows.net/{geoLocationSinkFolderName}"

# COMMAND ----------

import requests
import json
import pandas as pds

# COMMAND ----------

geoLocationAPIResponse = requests.get(geoLocationSourceAPIURL).json()
geoLocationPandasDF = pds.DataFrame(geoLocationAPIResponse)
geoLocationSparkDF = spark.createDataFrame(geoLocationPandasDF)

# COMMAND ----------

dailyPricingMarketNamesDF = spark.sql("SELECT MARKET_NAME from pricing_analytics.gold.reporting_dim_market_gold ")

# COMMAND ----------

# Fetch geolocation data for each market name and write results to the bronze layer in ADLS

from pyspark.sql.types import *

# Collect all market names from the Spark DataFrame into a Python list
marketNames = [dailyPricingMarketNames["MARKET_NAME"] for dailyPricingMarketNames in dailyPricingMarketNamesDF.collect()]

geoLocationAPIResponseList = []

# For each market name, call the geolocation API and collect the JSON response
for marketName in marketNames:
    # Construct the API URL for the current market name
    geoLocationSourceAPIURL = f"{geoLocationSourceAPIBaseURL}{marketName}{geoLocationSourceAPIURLOptions}"
    # Make the API request and parse the JSON response
    geoLocationAPIResponse = requests.get(geoLocationSourceAPIURL).json()
    # Append the response to the list if it is a dictionary (valid response)
    if isinstance(geoLocationAPIResponse, dict):
        geoLocationAPIResponseList.append(geoLocationAPIResponse)    

# Parallelize the list of API responses into an RDD for Spark processing
geoLocationSparkRDD = sc.parallelize(geoLocationAPIResponseList)

# Read the RDD of JSON objects into a Spark DataFrame
geoLocationSparkDF = spark.read.json(geoLocationSparkRDD)

# Filter out records where the 'admin1' field in 'results' is null and write the DataFrame to ADLS in JSON format
(geoLocationSparkDF
 .filter("results.admin1 IS NOT NULL")
 .write
 .mode("overwrite")
 .json(geoLocationSinkFolderPath))

# COMMAND ----------

from pyspark.sql.functions import col, array_contains
geoLocationBronzeDF = (spark
                       .read
                                             .json(geoLocationSinkFolderPath))
                       

display(geoLocationBronzeDF)