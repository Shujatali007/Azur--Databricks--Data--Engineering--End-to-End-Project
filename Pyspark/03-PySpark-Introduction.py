# Databricks notebook source
# MAGIC %md
# MAGIC ##### Source File Details
# MAGIC Source File URL : "https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01012023.csv"
# MAGIC
# MAGIC Source File Ingestion Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC ##### Python Core Library Documentation
# MAGIC - <a href="https://pandas.pydata.org/docs/user_guide/index.html#user-guide" target="_blank">pandas</a>
# MAGIC - <a href="https://pypi.org/project/requests/" target="_blank">requests</a>
# MAGIC - <a href="https://docs.python.org/3/library/csv.html" target="_blank">csv</a>
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>

# COMMAND ----------

# Configure Azure Storage Account Access
# NOTE: In production, use Azure Key Vault or Databricks Secrets for secure credential management
# storageAccountKey = dbutils.secrets.get(scope="your-scope", key="storage-account-key")
storageAccountName = "your-storage-account-name"
# spark.conf.set(f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net", storageAccountKey)

# COMMAND ----------

import pandas

# COMMAND ----------

sourceFileURL = 'https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01022023.csv'
# bronzelayerCSVFilePath = 'abfss://working-labs@adlsujadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/csv'


# COMMAND ----------

sourceFilePandasDF

# COMMAND ----------

sourceFilePandasDF

# COMMAND ----------

spark.createDataFrame(sourceFilePandasDF)

# COMMAND ----------

sourceFileSparkDF = spark.createDataFrame(sourceFilePandasDF)

# COMMAND ----------

print(sourceFilePandasDF )
display(sourceFileSparkDF)

# COMMAND ----------

sourceFileSparkDF.write.mode("overwrite").csv(bronzelayerCSVFilePath)

# COMMAND ----------

