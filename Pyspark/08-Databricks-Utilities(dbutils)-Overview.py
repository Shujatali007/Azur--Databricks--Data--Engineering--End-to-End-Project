# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC PARQUET  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
# MAGIC
# MAGIC
# MAGIC ##### Databricks Utilities
# MAGIC - <a href="https://docs.databricks.com/en/dev-tools/databricks-utils.html">dbutils</a>

# COMMAND ----------

# Configure Azure Storage Account Access
# NOTE: In production, use Azure Key Vault or Databricks Secrets for secure credential management
# storageAccountKey = dbutils.secrets.get(scope="your-scope", key="storage-account-key")
storageAccountName = "your-storage-account-name"
# spark.conf.set(f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net", storageAccountKey)

# COMMAND ----------

sourceCSVFilePath = 'abfss://working-labs@adlsudadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/csv'
sourceJSONFilePath = 'abfss://working-labs@adlsudadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/json'
sourcePARQUETFilePath = 'abfss://working-labs@adlsudadatalakehousedev.dfs.core.windows.net/bronze/daily-pricing/parquet'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

dbutils.fs.ls(sourceCSVFilePath)