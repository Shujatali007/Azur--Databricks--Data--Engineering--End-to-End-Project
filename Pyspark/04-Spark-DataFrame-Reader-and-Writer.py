# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON Target File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html" target="_blank">DataFrameReader</a>: **`csv`**,  **`option (header,separator)`** ,  **`schema`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html" target="_blank">SparkDataTypes</a>: **`ArrayType`**, **`DoubleType`**, **`IntegerType`**, **`LongType`**, **`StringType`**, **`StructType`**, **`StructField`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html" target="_blank">StructType</a>
# MAGIC
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrameWriter.json.html" target="_blank">DataFrameWriter</a>: **`json`**,  **`mode (overwrite,append)`** 

# COMMAND ----------

# Configure Azure Storage Account Access
# NOTE: In production, use Azure Key Vault or Databricks Secrets for secure credential management
# storageAccountKey = dbutils.secrets.get(scope="your-scope", key="storage-account-key")
storageAccountName = "your-storage-account-name"
# spark.conf.set(f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net", storageAccountKey)

# COMMAND ----------

sourceCSVFilePath = 'abfss://working-labs@adlsujadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/csv'
targetJSONFilePath = 'abfss://working-labs@adlsujadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/json'

# COMMAND ----------

sourceCSVFileDF = ( spark.
                   read.
                   option("header","true").
                   csv(sourceCSVFilePath) 
                   )

# COMMAND ----------

display (sourceCSVFileDF )

# COMMAND ----------

from pyspark.sql.types import *
sourceCSVFileSchema = StructType([
  StructField("DATE_OF_PRICING",StringType(),True),
  StructField("ROW_ID",IntegerType(),True),
  StructField("STATE_NAME",StringType(),True),
  StructField("MARKET_NAME",StringType(),True),
  StructField("PRODUCTGROUP_NAME",StringType(),True),
  StructField("PRODUCT_NAME",StringType(),True),
  StructField("VARIETY",StringType(),True),
  StructField("ORIGIN",StringType(),True),
  StructField("ARRIVAL_IN_TONNES",DecimalType(10,2),True),
  StructField("MINIMUM_PRICE",StringType(),True),
  StructField("MAXIMUM_PRICE",StringType(),True),
  StructField("MODAL_PRICE",StringType(),True),
  ])

# COMMAND ----------

sourceCSVFileDF = ( spark.
                   read.
                   schema(sourceCSVFileSchema).
                   csv(sourceCSVFilePath) 
                  )

# COMMAND ----------

display (sourceCSVFileDF )

# COMMAND ----------

sourceCSVFileDF.printSchema

# COMMAND ----------

targetJSONFilePath

# COMMAND ----------

sourceCSVFileDF

# COMMAND ----------

(sourceCSVFileDF.
 write.
 mode("overwrite").
 json(targetJSONFilePath)
 )

# COMMAND ----------

sourceCSVFilePath

# COMMAND ----------

targetJSONFilePath

# COMMAND ----------

sourceCSVFileDF

# COMMAND ----------

(sourceCSVFileDF.
 write.
 option("header","true").
 mode("overwrite").
 csv(sourceCSVFilePath)
 )

# COMMAND ----------

