# Databricks notebook source
query = """
SELECT *
FROM ambev_gold.gold_layer
"""

df = spark.sql(query)

# Storage account key
spark.conf.set("fs.azure.account.key.abinbevteste.dfs.core.windows.net", "Azure_key")

pathOutputTemp = "abfss://teste@abinbevteste.dfs.core.windows.net/CaminhoFinal"

df.coalesce(1).write \
    .format("csv") \
    .mode('overwrite') \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("nullValue", "") \
    .option("charset", "utf-8") \
    .save(pathOutputTemp)

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import datetime

# Nome do novo arquivo
nameFile = f"{datetime.datetime.today().strftime('%Y%m%d')} visao_final.csv"
fileMerge = f"abfss://teste@abinbevteste.dfs.core.windows.net/output/{nameFile}"

# Verifica se há arquivos na pasta de destino
files_in_destination = dbutils.fs.ls("abfss://teste@abinbevteste.dfs.core.windows.net/output/")
if files_in_destination:
    # Se houver, exclua o arquivo existente
    dbutils.fs.rm(files_in_destination[0].path, recurse=True) 

# Obtém o caminho do arquivo csv no diretório de saída temporária
file_path= list(filter(lambda a: a.name.endswith(".csv"), dbutils.fs.ls(pathOutputTemp)))[0].path

# Copia o novo arquivo para o destino final
dbutils.fs.cp(file_path, fileMerge)
# Remove o diretório de saída temporária
dbutils.fs.rm(pathOutputTemp, recurse=True)
