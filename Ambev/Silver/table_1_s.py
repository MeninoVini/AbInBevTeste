# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create database if not exists ambev_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Limpar e transformar os dados da camada Bronze e inseri-los na camada Silver
# MAGIC create or replace table ambev_silver.silver_layer
# MAGIC SELECT
# MAGIC     id,
# MAGIC     TRIM(name) AS name,
# MAGIC     TRIM(brewery_type) AS brewery_type,
# MAGIC     TRIM(address_1) AS address_1,
# MAGIC     COALESCE(TRIM(address_2), '') AS address_2,
# MAGIC     COALESCE(TRIM(address_3), '') AS address_3,
# MAGIC     UPPER(TRIM(city)) AS city,
# MAGIC     UPPER(TRIM(state_province)) AS state_province,
# MAGIC     TRIM(postal_code) AS postal_code,
# MAGIC     UPPER(TRIM(country)) AS country,
# MAGIC     CAST(TRIM(longitude) AS FLOAT) AS longitude,
# MAGIC     CAST(TRIM(latitude) AS FLOAT) AS latitude,
# MAGIC     REGEXP_REPLACE(TRIM(phone), '[^0-9]', '') AS phone,
# MAGIC     CASE
# MAGIC         WHEN website_url LIKE 'http%' THEN TRIM(website_url)
# MAGIC         ELSE NULL
# MAGIC     END AS website_url
# MAGIC FROM
# MAGIC     ambev_bronze.bronze_layer
# MAGIC WHERE
# MAGIC     -- Filtrar registros onde longitude e latitude não são nulos
# MAGIC     longitude IS NOT NULL AND latitude IS NOT NULL AND
# MAGIC     -- Adicionar outras condições de limpeza
# MAGIC     id IS NOT NULL AND
# MAGIC     name IS NOT NULL AND
# MAGIC     brewery_type IS NOT NULL AND
# MAGIC     address_1 IS NOT NULL AND
# MAGIC     city IS NOT NULL AND
# MAGIC     state_province IS NOT NULL AND
# MAGIC     postal_code IS NOT NULL AND
# MAGIC     country IS NOT NULL AND
# MAGIC     phone IS NOT NULL AND
# MAGIC     website_url IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ambev_silver.silver_layer
