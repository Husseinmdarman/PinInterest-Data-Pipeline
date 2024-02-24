# Databricks notebook source
# MAGIC %md
# MAGIC Cleaning the Geo dataframe

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
df_geo= table(global_temp_db + ".geo_uncleaned")

# COMMAND ----------

#Create a new column coordinates that contains an array based on the latitude and longitude columns
df_geo = df_geo.withColumn('coordinates', f.array(f.col("latitude"),f.col("longitude")))
#Drop the latitude and longitude columns from the DataFrame
df_geo =df_geo.drop(*["latitude", "longitude"])

# COMMAND ----------

#Convert the timestamp column from a string to a timestamp data type
df_geo = df_geo.withColumn("timestamp", f.to_timestamp("timestamp", format= "yyyy-MM-dd'T'HH:mm:ss"))

df_geo = df_geo.selectExpr('ind','country','coordinates','timestamp')

# COMMAND ----------

df_geo.createOrReplaceGlobalTempView("df_geo")
