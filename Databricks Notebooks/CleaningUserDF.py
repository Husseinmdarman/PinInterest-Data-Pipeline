# Databricks notebook source
# DBTITLE 1,DF_USER
# MAGIC %md
# MAGIC Cleaning df_user

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
df_user= table(global_temp_db + ".user_uncleaned")

# COMMAND ----------

#Create a new column user_name that concatenates the information found in the first_name and last_name columns
df_user = df_user.withColumn('user_name', f.concat(f.col("first_name"),f.col("last_name")))

#Drop the first_name and last_name columns from the DataFrame

df_user = df_user.drop(*["first_name", "last_name"])


# COMMAND ----------

#Convert the date_joined column from a string to a timestamp data type
df_user = df_user.withColumn("date_joined", f.to_timestamp("date_joined", format= "yyyy-MM-dd'T'HH:mm:ss"))
#Reorder the DataFrame columns to have the following column order: ind,user_name,age,date_joined
df_user = df_user.selectExpr('ind','user_name','age','date_joined')

# COMMAND ----------

df_user.createOrReplaceGlobalTempView("user")
