# Databricks notebook source
# Rutas external location
path_sql_app = "abfss://bronze@aprovidatalake.dfs.core.windows.net/datos_estructurados"
path_cosmos_web = "abfss://bronze@aprovidatalake.dfs.core.windows.net/datos_no_estructurados"

# Cargar a DataFrames
df_app = spark.read.parquet(path_sql_app)
df_web = spark.read.parquet(path_cosmos_web)

# COMMAND ----------

from pyspark.sql.functions import lit

# Añadimos la etiqueta de origen segun el canal
df_web_ready = df_web.withColumn("canal", lit("WEB"))
df_app_ready = df_app.withColumn("canal", lit("APP"))


# COMMAND ----------

display(df_web_ready)

# COMMAND ----------

display(df_app_ready)


# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat, lit, date_format

# al realiza un analisis de las fuente se identifica una diferencia de formato en la hora, por lo anterior se realiza una limpieza y estandarizacion de la hora.

# 2. Corregimos la columna 'hora' para extraer solo HH:mm:ss
# date_format convierte el timestamp largo de SQL en el string de hora
df_app_limpios = df_app_ready.withColumn("hora", date_format(col("hora"), "HH:mm:ss"))


# Unificamos ambas fuentes (Union)
datos_silver = df_web_ready.unionByName(df_app_limpios)



# 3. Aplicamos la lógica para la Capa Silver (Unificando con APP)
# aplicamos casteos solicitados en el requerimiento
df_final = datos_silver.select(
    lit("WEB").alias("canal"),
    col("id_trx"),
    col("id_usuario"),
    col("tipo_operacion").alias("operacion"),
    col("monto").cast("double"),
    # Usamos la hora_limpia para el concat
    to_timestamp(concat(col("fecha"), lit(" "), col("hora")), "yyyy-MM-dd HH:mm:ss").alias("fecha_hora_ts"),
    col("fecha"),
    col("hora").alias("hora")
)

# 3. Eliminar duplicados (id_trx, id_usuario)
df_final_silver = datos_silver.dropDuplicates(["id_trx", "id_usuario"])

display(df_final_silver)



# COMMAND ----------

# 1. ruta container storage (External Location)
path_silver = "abfss://silver@aprovidatalake.dfs.core.windows.net/"

# 2. Definimos la carpeta para esta tabla
folder = "silver_transacciones"
full_path = f"{path_silver}{folder}"

# Crear el esquema lógico si no existe directamente en databricks
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

df_final_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", full_path) \
    .saveAsTable("silver.silver_transacciones")

print("Tabla creada exitosamente en el catálogo y en el storage.")

# COMMAND ----------

#validamos datos en catalogo
display(spark.sql("SELECT * FROM silver.silver_transacciones LIMIT 10"))
