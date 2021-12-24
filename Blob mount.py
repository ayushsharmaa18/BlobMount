# Databricks notebook source
# MAGIC %md
# MAGIC ##Mount blob storage
# MAGIC ####You can mount a Blob storage container or a folder inside a container to DBFS. The mount is a pointer to a Blob storage container, so the data is never synced locally.

# COMMAND ----------

# To mount a Blob storage container or a folder inside a container, use the following command:
# dbutils.fs.mount(
#   source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
#   mount_point = "/mnt/<mount-name>",
#   extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})
dbutils.fs.mount(
  source = "wasbs://test-container@datalakeinazure.blob.core.windows.net",
  mount_point = "/mnt/test_mount",
  extra_configs = {"fs.azure.account.key.datalakeinazure.blob.core.windows.net":"p2nELMDB1lYNT7gbQHnpZMlxi33XXx2RDU7p//AOpEZfNXiuiHUe9UzCmNTrqjlhPPRGolOYGJtDqRgC1Mz4cw=="})

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Unmounting 
# MAGIC ##To unmount a mount point, use the following command:

# COMMAND ----------

# dbutils.fs.unmount("/mnt/<mount-name>")
dbutils.fs.unmount("/mnt/test-container")

# COMMAND ----------

df = spark.read.csv('/mnt/test_mount/userdata1.csv')
df.display()

# COMMAND ----------

# MAGIC %fs ls /mnt/test_mount

# COMMAND ----------

# df1 = df.filter()
df1 = df.sort("_c2").display()

# COMMAND ----------

