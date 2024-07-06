# Databricks notebook source
# Définition des crédentials
aws_access_key = "" 
aws_secret_key = ""
encoded_secret_key = aws_secret_key.replace("/", "%2F")

# Variabilisations
s3_bucket = "running-activity-aog"
mount_name = "/mnt/running-activity-aog"

# Montage du S3 bucket pour y avoir accès
dbutils.fs.mount(
  source = f"s3a://{aws_access_key}:{encoded_secret_key}@{s3_bucket}",
  mount_point = mount_name,
  extra_configs = {"fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"}
)


# COMMAND ----------

# Démontage du S3 bucket
# dbutils.fs.unmount("/mnt/running-activity-aog")

# COMMAND ----------

df = spark.read.csv("/mnt/running-activity-aog/my_activity_data_20240705184527.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "{{file_name}}"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `{{file_name}}`

# COMMAND ----------

# Since this table is registered as a temp view, it will only be available to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "{{table_name}}"

# df.write.format("{{table_import_type}}").saveAsTable(permanent_table_name)
