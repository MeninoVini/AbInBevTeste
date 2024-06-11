# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists ambev_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Definindo como "replace" para que não haver duplicidade ou acumulo
# MAGIC CREATE OR REPLACE TABLE ambev_gold.gold_layer (
# MAGIC     state_province STRING,
# MAGIC     brewery_type STRING,
# MAGIC     brewery_count INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agregar e transformar os dados da camada Silver para a camada Gold
# MAGIC INSERT INTO ambev_gold.gold_layer
# MAGIC SELECT
# MAGIC     state_province,
# MAGIC     brewery_type,
# MAGIC     COUNT(id) AS brewery_count
# MAGIC FROM
# MAGIC     ambev_silver.silver_layer
# MAGIC GROUP BY
# MAGIC     state_province,
# MAGIC     brewery_type;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ambev_gold.gold_layer
