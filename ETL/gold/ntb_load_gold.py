# Databricks notebook source
# Rutas external location
path_silver = "abfss://silver@aprovidatalake.dfs.core.windows.net/silver_transacciones"
# Cargar a DataFrames
datos_silver = spark.read.parquet(path_silver)


# COMMAND ----------

df_silver = spark.read.format("delta").load("abfss://silver@aprovidatalake.dfs.core.windows.net/silver_transacciones")

# COMMAND ----------

# lectura de datos desde el external location (storage)
display(df_silver)

# COMMAND ----------

# 
# en el proceso anterior se almaceno directamente en el catalogo paralectura directa
display(spark.sql("SELECT * FROM silver.silver_transacciones LIMIT 10"))
$0en el proceso anterior se almaceno directamente en el catalogo paralectura directa
display(spark.sql("SELECT * FROM silver.silver_transacciones LIMIT 10"))

# COMMAND ----------

# Crear el esquema lógico para la capa Gold
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

from pyspark.sql.functions import col, year, month, dayofmonth, monotonically_increasing_id, format_string

# --- DIMENSIÓN CANAL ---
# Extraemos los valores únicos de canal tabla Silver
df_canales = spark.read.table("silver.silver_transacciones").select("canal").distinct()
dim_canal = df_canales.withColumn("sk_canal", monotonically_increasing_id())

# --- DIMENSIÓN FECHA ---
# Creamos una dimensión fecha a partir de las fechas existentes
df_fechas = spark.read.table("silver.silver_transacciones").select("fecha").distinct()
dim_fecha = df_fechas.select(
    format_string("%d%02d%02d", year("fecha"), month("fecha"), dayofmonth("fecha")).cast("int").alias("sk_fecha"),
    col("fecha"),
    year("fecha").alias("anio"),
    month("fecha").alias("mes"),
    dayofmonth("fecha").alias("dia")
).orderBy("sk_fecha")

# Guardar dimensiones en Gold
dim_canal.write.format("delta").mode("overwrite").saveAsTable("gold.dim_canal")
dim_fecha.write.format("delta").mode("overwrite").saveAsTable("gold.dim_fecha")
display(dim_canal)
display(dim_fecha)

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, min, max

# 1. creamos nuestra tabla de hechos a partir de las transacciones en la capa Silver y unimos con dimensiones para obtener los SK
df_silver = spark.read.table("silver.silver_transacciones")

df_maestra = df_silver.join(dim_canal, "canal") \
                      .join(dim_fecha, "fecha")

# 2. Generar Agregaciones Analíticas
fact_transacciones = df_maestra.groupBy("sk_canal", "sk_fecha", "tipo_operacion", "anio", "mes") \
    .agg(
        count("id_trx").alias("cantidad"),
        sum("monto").alias("sum_monto"),
        avg("monto").alias("avg_monto"),
        min("monto").alias("monto_min"),
        max("monto").alias("monto_max")
    )

# 3. Guardar Tabla de Hechos en Gold
path_gold_fact = "abfss://gold@aprovidatalake.dfs.core.windows.net/fact_transacciones"

fact_transacciones.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", path_gold_fact) \
    .saveAsTable("gold.fact_transacciones")

display(fact_transacciones)
