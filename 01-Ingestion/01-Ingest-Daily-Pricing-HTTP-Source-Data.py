# Databricks notebook source
# MAGIC %md
# MAGIC ####Notebook Name : 01-Ingest-Daily-Pricing-HTTP-Source-Data
# MAGIC ##### Source File Details
# MAGIC Source File URL : "https://retailpricing.blob.core.windows.net/daily-pricing"
# MAGIC
# MAGIC Source File Ingestion Path : "abfss://bronze@datalakestorageaccountname.dfs.core.windows.net/daily-pricing/"
# MAGIC
# MAGIC ##### Python Core Library Documentation
# MAGIC - <a href="https://pandas.pydata.org/docs/user_guide/index.html#user-guide" target="_blank">pandas</a>
# MAGIC - <a href="https://pypi.org/project/requests/" target="_blank">requests</a>
# MAGIC - <a href="https://docs.python.org/3/library/csv.html" target="_blank">csv</a>
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>

# COMMAND ----------

# Get the process name from Databricks widget
processName = dbutils.widgets.get('prm_processName')

# Build SQL to get the next source file date for the process, defaulting to '2023-01-01' if none found
nextSourceFileDateSql = f"""SELECT NVL(MAX(PROCESSED_FILE_TABLE_DATE)+1,'2023-01-01')  as NEXT_SOURCE_FILE_DATE FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE PROCESS_NAME = '{processName}' and PROCESS_STATUS='Completed'"""

# Execute the SQL and display the result
nextSourceFileDateDF = spark.sql(nextSourceFileDateSql)
display(nextSourceFileDateDF)

# COMMAND ----------

from datetime import datetime


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC File Url: sourceFileURL = 'https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01022023.csv'

# COMMAND ----------


dailyPricingSourceBaseURL = 'https://retailpricing.blob.core.windows.net/'
dailyPricingSourceFolder = 'daily-pricing/'
daiilyPricingSourceFileDate = datetime.strptime(str(nextSourceFileDateDF.select('NEXT_SOURCE_FILE_DATE').collect()[0]['NEXT_SOURCE_FILE_DATE']),'%Y-%m-%d').strftime('%m%d%Y')
daiilyPricingSourceFileName = f"PW_MW_DR_{daiilyPricingSourceFileDate}.csv"


daiilyPricingSinkLayerName = 'bronze'
daiilyPricingSinkStorageAccountName = 'adlsujadatalakehousedev'
daiilyPricingSinkFolderName =  'daily-pricing'



# COMMAND ----------

import pandas as pds

# COMMAND ----------

dailyPricingSourceURL = dailyPricingSourceBaseURL + dailyPricingSourceFolder + daiilyPricingSourceFileName
print(dailyPricingSourceURL)

dailyPricingPandasDF = pds.read_csv(dailyPricingSourceURL)
print(dailyPricingPandasDF)


# COMMAND ----------

dailyPricingSparkDF =  spark.createDataFrame(dailyPricingPandasDF)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
dailyPricingSinkFolderPath = f"abfss://{daiilyPricingSinkLayerName}@{daiilyPricingSinkStorageAccountName}.dfs.core.windows.net/{daiilyPricingSinkFolderName}"


(
    dailyPricingSparkDF
    .withColumn("source_file_load_date",current_timestamp())
    .write
    .mode("append")
    .option("header","true")
    .csv(dailyPricingSinkFolderPath)

)


# COMMAND ----------


processFileDate = nextSourceFileDateDF.select('NEXT_SOURCE_FILE_DATE').collect()[0]['NEXT_SOURCE_FILE_DATE']
processStatus ='Completed'

processInsertSql = f""" INSERT INTO pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_FILE_TABLE_DATE,PROCESS_STATUS) VALUES('{processName}','{processFileDate}','{processStatus}')"""

spark.sql(processInsertSql)




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS

# COMMAND ----------

