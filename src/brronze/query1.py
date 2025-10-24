# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Bronze Query
# MAGIC ***

# COMMAND ----------

test = spark.read.table("mlops_dev.marvel_characters.test_set")
train = spark.read.table("mlops_dev.marvel_characters.train_set")
df = train.union(test)
display(df)
# df.write.mode("overwrite").saveAsTable("mlops_dev.bronze.marvel_characters")
