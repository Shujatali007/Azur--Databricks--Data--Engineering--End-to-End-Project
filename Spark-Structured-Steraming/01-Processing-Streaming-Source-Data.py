# Databricks notebook source
# MAGIC %md
# MAGIC # Reading from a Streaming Source Data and Writing
# MAGIC
# MAGIC ***Notebook Name*** : 01-Processing-Streaming-Source-Data
# MAGIC
# MAGIC ***Source Stream Data Schema*** : "ARRIVAL_IN_TONNES double,DATETIME_OF_PRICING string ,MARKET_NAME string,MAXIMUM_PRICE double,MINIMUM_PRICE double,MODAL_PRICE double,ORIGIN string,PRODUCTGROUP_NAME string,PRODUCT_NAME string,ROW_ID long,STATE_NAME string,VARIETY string,source_stream_load_datetime string"
# MAGIC
# MAGIC ##### Structured Streaming Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

sourceStreamJSONFilePath = 'abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-source-data'
sinkStreamJSONFilePath = 'abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-data/json'

# COMMAND ----------

sourceStreamJSONFileDF = (spark
                          .readStream
                          .schema("ARRIVAL_IN_TONNES double,DATETIME_OF_PRICING string ,MARKET_NAME string,MAXIMUM_PRICE double,MINIMUM_PRICE double,MODAL_PRICE double,ORIGIN string,PRODUCTGROUP_NAME string,PRODUCT_NAME string,ROW_ID long,STATE_NAME string,VARIETY string,source_stream_load_datetime string")
                          .format("json")
                          .load(sourceStreamJSONFilePath))


# COMMAND ----------

(sourceStreamJSONFileDF
 .writeStream
 .format("json")
 .start(sinkStreamJSONFilePath)
)

# COMMAND ----------

sinkStreamJSONcheckpointPath = 'abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-data/json/checkpoint'

streamProcessingQuery = (sourceStreamJSONFileDF
 .writeStream
 .outputMode("append")
 .format("json")
 .queryName("stream-processing")
 .trigger(processingTime = "5 Minutes")
 .option("checkpointLocation", sinkStreamJSONcheckpointPath)
 .start(sinkStreamJSONFilePath)
)

# COMMAND ----------

streamProcessingQuery.id

# COMMAND ----------

streamProcessingQuery.status

# COMMAND ----------

streamProcessingQuery.lastProgress

# COMMAND ----------

StreamDF = (spark
             .read
             .format("json")
             .load(sinkStreamJSONFilePath)
             )

display(StreamDF.count())

# COMMAND ----------

streamProcessingQuery.stop()

# COMMAND ----------

