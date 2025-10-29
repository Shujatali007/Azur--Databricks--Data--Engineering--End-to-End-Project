# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC source API URL : "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2023-01-01&end_date=2024-01-01&daily=temperature_2m_max,temperature_2m_min,rain_sum"
# MAGIC
# MAGIC JSON Target File Path : "abfss://bronze@datalakestorageaccountname.dfs.core.windows.net/weather-data/
# MAGIC "

# COMMAND ----------

weatherDataSourceAPIURL = "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2023-01-01&end_date=2024-01-01&daily=temperature_2m_max,temperature_2m_min,rain_sum"

weatherDataSourceAPIBaseURL = "https://archive-api.open-meteo.com/v1/archive?latitude="
weatherDataSourceAPIURLOptions = "&daily=temperature_2m_max,temperature_2m_min,rain_sum"

weatherDataSinkLayerName = 'bronze'
weatherDataSinkStorageAccountName = 'adlsujadatalakehousedev'
weatherDataSinkFolderName = 'weather-data'

weatherDataSinkFolderPath = f"abfss://{weatherDataSinkLayerName}@{weatherDataSinkStorageAccountName}.dfs.core.windows.net/{weatherDataSinkFolderName}"

# COMMAND ----------

import requests
import json
import pandas as pds

# COMMAND ----------

geoLocationsDF = spark.sql("SELECT latitude,longitude,marketName from pricing_analytics.silver.geo_location_silver LIMIT 100 ")
display(geoLocationsDF.count())

# COMMAND ----------


# Initialize an empty list to store weather API responses for each geo location
weatherDataAPIResponseList = []
for geoLocations in geoLocationsDF.collect():
  # For each geo location, construct the API URL with specific latitude and longitude
  weatherDataSourceAPIURL = f"{weatherDataSourceAPIBaseURL}{geoLocations['latitude']}&longitude={geoLocations['longitude']}&start_date=2023-01-01&end_date=2023-12-31{weatherDataSourceAPIURLOptions}"
  # Make a GET request to the weather API and parse the response as JSON
  weatherDataAPIResponse = requests.get(weatherDataSourceAPIURL).json()
  # Add the market name to the API response for traceability
  weatherDataAPIResponse["marketName"] = geoLocations["marketName"]
  # Convert the API response dictionary to a JSON string
  weatherDataAPIResponseJson = json.dumps(weatherDataAPIResponse)
  # Only append the response if it is a dictionary (valid API response)
  if isinstance(weatherDataAPIResponse, dict):
   weatherDataAPIResponseList.append(weatherDataAPIResponseJson)

# Parallelize the list of JSON strings into an RDD for distributed processing
weatherDataRDD = sc.parallelize(weatherDataAPIResponseList)

# Read the RDD of JSON strings into a Spark DataFrame
weatherDataSparkDF = spark.read.json(weatherDataRDD)

# Write the resulting DataFrame to the specified storage path in JSON format, overwriting any existing data
(weatherDataSparkDF
.write
.mode("overwrite")
.json(weatherDataSinkFolderPath))  