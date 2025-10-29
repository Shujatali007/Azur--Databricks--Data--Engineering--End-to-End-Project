# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC PARQUET Target  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
# MAGIC  
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameReader</a>: **`json`**,**`csv`**,  **`option (header,inferSchema)`** ,  **`schema`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">PySparkSQLFunctions</a>: **`col`** , **`alias`** , **`cast`**
# MAGIC
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> transformations and actions: 
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`limit`** , **`take`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |
# MAGIC
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`display`**, **`createOrReplaceTempView`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameWriter</a>: **`parquet`**, **`csv`**, **`mode (overwrite,append)`** 

# COMMAND ----------

# Configure Azure Storage Account Access
# NOTE: In production, use Azure Key Vault or Databricks Secrets for secure credential management
# storageAccountKey = dbutils.secrets.get(scope="your-scope", key="storage-account-key")
storageAccountName = "your-storage-account-name"
# spark.conf.set(f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net", storageAccountKey)

# COMMAND ----------

sourceCSVFilePath = 'abfss://working-labs@adlsujadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/csv'
targetPARQUETFilePath = 'abfss://working-labs@adlsujadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/parquet'

# COMMAND ----------

sourceCSVFileDF = (spark.
                   read.
                   load(sourceCSVFilePath , format = "csv" )
)

# COMMAND ----------

sourceCSVFileDF = (spark.
                   read.
                   load(sourceCSVFilePath , format = "csv"  , header = "true")
)

# COMMAND ----------

sourceCSVFileDF = (spark.
                   read.
                   load(sourceCSVFilePath , format = "csv"  , header = "true" , inferSchema = "true")
)

# COMMAND ----------

from pyspark.sql.functions import col
(sourceCSVFileDF.
 withColumn("ARRIVALS_IN_KILOGRAMS" , col("ARRIVAL_IN_TONNES") * 1000)
)

# COMMAND ----------

sourceCSVFileTransDF = (sourceCSVFileDF.
 withColumn("ARRIVALS_IN_KILOGRAMS" , col("ARRIVAL_IN_TONNES") * 1000)
 )

# COMMAND ----------

display(sourceCSVFileTransDF)

# COMMAND ----------

from pyspark.sql.functions import sum
(sourceCSVFileTransDF.
groupBy("STATE_NAME","PRODUCT_NAME").
agg(sum("ARRIVALS_IN_KILOGRAMS")).
        show())


# COMMAND ----------

(sourceCSVFileTransDF.
groupBy("STATE_NAME","PRODUCT_NAME").
agg(sum("ARRIVALS_IN_KILOGRAMS").alias("TOTAL_ARRIVALS_IN_KILOGRAMS")).
        show())

# COMMAND ----------

(sourceCSVFileTransDF.
groupBy("STATE_NAME","PRODUCT_NAME").
agg(sum("ARRIVALS_IN_KILOGRAMS").alias("TOTAL_ARRIVALS_IN_KILOGRAMS")).
orderBy("TOTAL_ARRIVALS_IN_KILOGRAMS").
        show())

# COMMAND ----------

from pyspark.sql.functions import desc
(sourceCSVFileTransDF.
groupBy("STATE_NAME","PRODUCT_NAME").
agg(sum("ARRIVALS_IN_KILOGRAMS").alias("TOTAL_ARRIVALS_IN_KILOGRAMS")).
orderBy(desc("TOTAL_ARRIVALS_IN_KILOGRAMS")).
        show())

# COMMAND ----------

(sourceCSVFileDF
 .write
  .save(targetPARQUETFilePath))

# COMMAND ----------

(spark
.read
.load(targetPARQUETFilePath)
.show()
)

# COMMAND ----------

