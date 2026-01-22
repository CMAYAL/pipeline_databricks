# Databricks notebook source
#instalamos libreria de cosmos para facililitar la lectura
%pip install azure-cosmos

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.secrets.list(scope="scope-dev")

# COMMAND ----------

endpoint=dbutils.secrets.get(scope="scope-dev", key="secret-endpoint-cosmodb")
key=dbutils.secrets.get(scope="scope-dev", key="secret-conector-key")
name_database="aprovidbweb"
name_container ="transacciones_web"

# COMMAND ----------

from azure.cosmos import CosmosClient as cosmodb
conect_url = cosmodb(endpoint,key)
db = conect_url.get_database_client(name_database)
container=db.get_container_client(name_container)

# COMMAND ----------

list_items = list(container.read_all_items(max_item_count=30))
df = spark.createDataFrame(list_items)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Aplicamos la misma estructura que se uso para SQL
datos_cosmos_formateados = df.select(
    col("id_trx").cast("int"),
    col("id_usuario"),
    col("codigo_operacion").alias("tipo_operacion"), # me quedo mal el nombre en cosmos , por eso lo ajuste desde aqui
    col("monto").cast("decimal(18,2)"),
    col("fecha"),
    col("hora")
).orderBy("fecha")

# Visualizar el resultado final
display(datos_cosmos_formateados)

# COMMAND ----------

# 1. Definimos la ruta del storage (External Location)
path = "abfss://bronze@aprovidatalake.dfs.core.windows.net/"

# 2. Definimos la carpeta tabla datos origen no estructurados
folder = "datos_no_estructurados"
full_path = f"{path}{folder}"

# 3. Escribimos en formato Parquet
datos_cosmos_formateados.write \
    .mode("overwrite") \
    .parquet(full_path)

print(f"Escritura completada en: {full_path}")

# COMMAND ----------

# validamos carga de datos en capa bronce
dbutils.fs.ls("abfss://bronze@aprovidatalake.dfs.core.windows.net/datos_no_estructurados")
