# Databricks notebook source
# MAGIC %md
# MAGIC ## Api ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC - ## import the library 

# COMMAND ----------

# DBTITLE 1,library
import requests 
import json 
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **create catalog**

# COMMAND ----------

# DBTITLE 1,Create Catalog , Schema and Volume

spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")


base_path='/Volumes/workspace/default/csv'


# COMMAND ----------

# MAGIC %md
# MAGIC Calling cricket Api

# COMMAND ----------

###CALLING Cricket API
API_KEY='4e60c588-2294-4d25-9cdf-31705f1fda7c'
api_url=f"https://api.cricapi.com/v1/countries?apikey=4e60c588-2294-4d25-9cdf-31705f1fda7c&offset=0"

response =requests.get(api_url)
response.raise_for_status()

api_data=response.json()
print(api_data.keys())



# COMMAND ----------

print(json.dumps(api_data,indent= 2)[:2000])

# COMMAND ----------

# MAGIC %md
# MAGIC **SAVE RAW API Response in the Volumes**

# COMMAND ----------

raw_file_path=f'{base_path}/current_matches_raw.json'

with open (raw_file_path,'w') as file:
    json.dump(api_data,file)

print("RAW API data is save at the :",raw_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **create bronze layer**

# COMMAND ----------

bronze_data=[{
"source_api":api_url,
"raw_json":json.dumps(api_data),
"ingestion_time":None
}]

# COMMAND ----------

bronze_schema=StructType([
StructField("source_api",StringType(),True),
StructField("raw_json",StringType(),True),
StructField("ingestion_time",TimestampType(),True)
])


# COMMAND ----------

bronze_df=spark.createDataFrame(bronze_data,bronze_schema)\
    .withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a bronze layer**

# COMMAND ----------

bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")

print("BRONZE TABLE CREATED SUCCESSFULLY")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_bronze_current_matches 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC