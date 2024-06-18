# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists ambev_bronze 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- criando tabela Delta caso não exista
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ambev_bronze.bronze_layer
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Realiza a leitura, transformação e geração de tabela temporária (df) que será utilizada para carga final</h6>
# MAGIC
# MAGIC Configura o timeZone de São Paulo na sessão do Spark e utiliza a função **expr** para gravação de data/hora atual (current_timestamp)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Configurações de acesso ao Azure Blob Storage
spark.conf.set("fs.azure.account.key.abinbevteste.blob.core.windows.net", "/2nNjzZ6SGm7ZOW8lcMxZa6kfNed57HmZ0djdHmbtvlrGbUpwBI5xIeg4EnEkTbaNReLdkYkemBO+AStUnLJ8A==")

# Caminho do arquivo JSON no Azure Blob Storage
blob_path = "wasbs://teste@abinbevteste.blob.core.windows.net/dir_teste/breweryTable"

# Realiza leitura do arquivo JSON
df = spark.read.json(blob_path)

# Adicionando data_processamento ao conjunto de dados
spark.conf.set("spark.sql.session.timeZone","America/Sao_Paulo") #definindo timeZone
df = df.withColumn("data_processamento", expr("current_timestamp()"))

# Cria ou substitui a view temporária
df.createOrReplaceTempView("df")

# Exibindo resultado
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Realiza o replace dos dados na tabela final
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ambev_bronze.bronze_layer AS
# MAGIC
# MAGIC SELECT * FROM df