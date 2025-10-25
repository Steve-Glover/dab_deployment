# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Bronze Query
# MAGIC ***

# COMMAND ----------

from databricks.connect import DatabricksSession
from IPython.display import display

# Fully declare spark session
spark = DatabricksSession.builder.getOrCreate()

# Testing Connection
spark.sql("DROP TABLE IF EXISTS mlops_dev.bronze.marvel_characters")

test = spark.read.table("mlops_dev.marvel_characters.test_set")  
train = spark.read.table("mlops_dev.marvel_characters.train_set") 
df = train.union(test)
display(df) 
df.write.mode("overwrite").saveAsTable("mlops_dev.bronze.marvel_characters")



# COMMAND ----------
