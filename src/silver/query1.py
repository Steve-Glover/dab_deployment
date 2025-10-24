# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Silver Query
# MAGIC ***
# COMMAND ----------
silver = spark.read.table("mlops_dev.bronze.marvel_characters")
lower_cols = [col.lower() for col in silver.columns]
silver = silver.toDF(*lower_cols)
silver.write.mode("overwrite").saveAsTable("mlops_dev.silver.marvel_characters")

# COMMAND ----------
display(spark.read.table("mlops_dev.silver.marvel_characters"))


# COMMAND ----------
