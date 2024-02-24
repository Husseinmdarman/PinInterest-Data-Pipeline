# Databricks notebook source
# DBTITLE 1,Pin Dataframe
# MAGIC %md
# MAGIC Cleaning the PIN dataframe

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
df_pin= table(global_temp_db + ".pin_uncleaned")

# COMMAND ----------

# No relevant pieces of data contained in a dictionary for each column in pin

pin_df_non_relevant_entries = {
  "description": "No description available",
  "follower_count": "User Info Error",
  "img_src": "Image src error.",
  "poster_name": "User Info Error",
  "tag_list": "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
  "title": "No Title Data Available"
}

# COMMAND ----------

for column in df_pin.columns: #the column names 
    if (column in pin_df_non_relevant_entries): #checks whether that column name has any specificied entry in non-relevant data
        df_pin = df_pin.withColumn(f"{column}", f.when(f.col(f"{column}") == pin_df_non_relevant_entries[column], None).otherwise(f.col(f"{column}"))) #finds the column entry value where its equal to the non-relevant data and changes it to None otherwise it keeps the same value

#due to the in feature being unsuupported the second piece of non-relevant data needs its own line
df_pin = df_pin.withColumn("description", f.when(f.col("description") == "No description available Story format", None).otherwise(f.col("description"))) 


# COMMAND ----------

#Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int, the int count has K representing 3 zeros and M representing 6 zeros therefore needs replacing before casting

df_pin = df_pin.withColumn("follower_count", f.regexp_replace('follower_count', r'[k]', '000')) #replaces the value of K with 3 zeros
df_pin = df_pin.withColumn("follower_count", f.regexp_replace('follower_count', r'[M]', '000000')) #replaces the value of M with 6 zeros
df_pin =df_pin.withColumn("follower_count", f.col('follower_count').cast("int")) #can cast the column to a interger now that all values in the string are in a numeric form

# COMMAND ----------

#Ensure that each column containing numeric data has a numeric data type
df_pin

# COMMAND ----------

#Clean the data in the save_location column to include only the save location path
df_pin = df_pin.withColumn("save_location", f.regexp_replace('save_location','Local save in ', ''))

# COMMAND ----------

#Rename the index column to ind.
df_pin = df_pin.withColumnRenamed('index', 'ind')

# COMMAND ----------

#Reorder the DataFrame columns to have the following column order:ind unique_id title
#description follower_count poster_name tag_list is_image_or_video image_src save_location
#category

df_pin = df_pin.selectExpr("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")


# COMMAND ----------

df_pin.createOrReplaceGlobalTempView("df_pin")
