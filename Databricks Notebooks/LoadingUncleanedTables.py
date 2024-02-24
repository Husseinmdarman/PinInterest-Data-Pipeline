# Databricks notebook source
# MAGIC %sql
# MAGIC /* turn-off format checks during the reading of Delta tables */
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

#location for files I would like to read from
file_location_geo = "/mnt/mount_name_hussein/topics/12e255fc4fcd.geo/partition=0/*.json"
file_location_user = "/mnt/mount_name_hussein/topics/12e255fc4fcd.user/partition=0/*.json"
file_location_pin = "/mnt/mount_name_hussein/topics/12e255fc4fcd.pin/partition=0/*.json"

file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_geo)

df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_user)

df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_pin)

# COMMAND ----------


#create global temp views of each uncleaned table to be used for their respective
#cleaning notebooks
df_user.createOrReplaceGlobalTempView("user_uncleaned")
df_geo.createOrReplaceGlobalTempView("geo_uncleaned")
df_pin.createOrReplaceGlobalTempView("pin_uncleaned")
