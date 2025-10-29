# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameReader</a>: **`json`**,**`csv`**,  **`option (header,inferSchema)`** ,  **`schema`**
# MAGIC
# MAGIC
# MAGIC ##### DateTime Methods
# MAGIC
# MAGIC **`current_timestamp()`** records the timestamp when the code is executed
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">ColumnFunctions</a>: **`cast`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">Built-In DateTime Functions</a>: **`date_format`**, **`to_date`**,**`year`**, **`month`**, **`dayofmonth`**, **`minute`**, **`second`**
# MAGIC
# MAGIC ##### Date Time Functions
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`current_timestamp`** | Returns the current timestamp at the start of query evaluation as a timestamp column |
# MAGIC | **`date_format`** | Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument. |
# MAGIC | **`dayofmonth`** | Extracts the day of the month as an integer from a given date/timestamp/string |
# MAGIC
# MAGIC
# MAGIC #####  Date Time Formats
# MAGIC | Format | Meaning         | DataType | Sample Output              |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | y      | year            | year         | 2020; 20               |
# MAGIC | D      | day-of-year     | number(3)    | 189                    |
# MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
# MAGIC | d      | day-of-month    | number(3)    | 28                     |
# MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |
# MAGIC

# COMMAND ----------

# Configure Azure Storage Account Access
# NOTE: In production, use Azure Key Vault or Databricks Secrets for secure credential management
# storageAccountKey = dbutils.secrets.get(scope="your-scope", key="storage-account-key")
storageAccountName = "your-storage-account-name"
# spark.conf.set(f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net", storageAccountKey)

# COMMAND ----------

sourceCSVFilePath = 'abfss://working-labs@adlsujadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/csv'
sourcePARQUETFilePath = 'abfss://working-labs@adlsujadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/parquet'

# COMMAND ----------

sourcePARQUETFileDF = (spark
                       .read
                       .load(sourcePARQUETFilePath)
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

sourcePARQUETTransFileDF = (sourcePARQUETFileDF
 .withColumn("datalake_file_load_date", current_timestamp())
             )

# COMMAND ----------

display(sourcePARQUETTransFileDF)

# COMMAND ----------

from pyspark.sql.functions import year ,  quarter
(sourcePARQUETTransFileDF
.select("datalake_file_load_date")
.withColumn("datalake_file_load_date_year" , year("datalake_file_load_date"))
.show()
)

# COMMAND ----------

from pyspark.sql.functions import month , dayofmonth
(sourcePARQUETTransFileDF
.select("datalake_file_load_date")
.withColumn("datalake_file_load_date_year" , year("datalake_file_load_date"))
.withColumn("datalake_file_load_date_month" , month("datalake_file_load_date"))
.withColumn("datalake_file_load_date_day" , dayofmonth("datalake_file_load_date"))
.show()
)

# COMMAND ----------

from pyspark.sql.functions import concat 
(sourcePARQUETTransFileDF
.select("datalake_file_load_date")
.withColumn("dl_file_load_date_format" , concat(year("datalake_file_load_date"), month("datalake_file_load_date"),dayofmonth("datalake_file_load_date")))
.show()
)

# COMMAND ----------

from pyspark.sql.functions import date_format
(sourcePARQUETTransFileDF
.select("datalake_file_load_date")
.withColumn("dl_file_load_date_format" , date_format("datalake_file_load_date","yyyyMMdd"))
.show()
)

# COMMAND ----------

from pyspark.sql.functions import to_date
(sourcePARQUETFileDF
.select("DATE_OF_PRICING")
.withColumn("PRICING_DATE" , to_date("DATE_OF_PRICING","dd/MM/yyyy"))
.show()
)

# COMMAND ----------

(sourcePARQUETFileDF
.select("DATE_OF_PRICING")
.withColumn("PRICING_DATE" , to_date("DATE_OF_PRICING","dd/MM/yyyy"))
.withColumn("PRICING_DATE_FORMAT",date_format("PRICING_DATE","yyyyMMdd"))
.show()
)


# COMMAND ----------

