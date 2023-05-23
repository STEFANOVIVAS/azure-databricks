# Databricks notebook source
# MAGIC %md
# MAGIC ### Acess azure data lake using Service Principal
# MAGIC  - Register Azure AD application / Service Principal
# MAGIC  - Generate a secret/password for the Application
# MAGIC  - Set spark config with APP/Client id, Directory/Tenant id & Secret
# MAGIC  - Assign role "Storage Blob Data Contributor" to the data lake.

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

client_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id')
client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1projectlake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1projectlake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1projectlake.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1projectlake.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1projectlake.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1projectlake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1projectlake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


