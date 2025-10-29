# Databricks notebook source
# MAGIC %md
# MAGIC ####Notebook Name : 03-Ingest-Pricing-Reference-DB-Source-Data
# MAGIC ##### Source Table Details
# MAGIC Source Tables:
# MAGIC masterdata.market_address
# MAGIC masterdata.country_profile
# MAGIC masterdata.exchange_rates
# MAGIC masterdata.domestic_product_codes
# MAGIC masterdata.global_item_codes
# MAGIC
# MAGIC
# MAGIC ##### Source Tables Ingestion Path In Bronze Layer:
# MAGIC  "abfss://bronze@datalakestorageaccountname.dfs.core.windows.net/reference-data/"
# MAGIC

# COMMAND ----------

pricingReferenceSourceTableName = dbutils.widgets.get("prm_pricingReferenceSourceTableName")

pricingReferenceSinkLayerName = 'bronze'
pricingReferenceSinkStorageAccountName = 'adlsujadatalakehousedev'
pricingReferenceSinkFolderName = 'reference-data'

# COMMAND ----------

# MAGIC %md
# MAGIC Lookup tables that standardize product names, market codes, state, and country information.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC /
# MAGIC This JDBC connection URL is used to connect to an Azure SQL Database named asqludacourses, which is hosted on the asqludacoursesserver server. The connection uses the user sourcereader with the password DBReader@2024 and has encryption enabled for secure data transfer. This setup is typically used for securely reading data from a SQL Server database into Databricks or other data processing environments on Azure.
# MAGIC
# MAGIC

# COMMAND ----------


JDBCconnectionUrl = "jdbc:sqlserver://asqludacoursesserver.database.windows.net;encrypt=true;databaseName=asqludacourses;user=sourcereader;password=DBReader@2024";

print(JDBCconnectionUrl)



pricingReferenceSourceTableDF =  (spark
                                 .read
                                 .format('jdbc')
                                 .option("url",JDBCconnectionUrl)
                                 .option("dbtable",pricingReferenceSourceTableName)
                                 .load()
)



# COMMAND ----------

pricingReferenceSinkTableFolder = pricingReferenceSourceTableName.replace('.','/')

# COMMAND ----------

pricingReferenceSinkFolderPath = f"abfss://{pricingReferenceSinkLayerName}@{pricingReferenceSinkStorageAccountName}.dfs.core.windows.net/{pricingReferenceSinkFolderName}/{pricingReferenceSinkTableFolder}"

(
    pricingReferenceSourceTableDF
    .write
    .mode("overwrite")
    .json(pricingReferenceSinkFolderPath)
)

