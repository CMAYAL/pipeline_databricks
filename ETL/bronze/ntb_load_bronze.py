# Databricks notebook source
dbutils.secrets.listScopes()


# COMMAND ----------

# DBTITLE 1,Cell 4
dbutils.secrets.list(scope="scope-dev")


# COMMAND ----------

user=dbutils.secrets.get(scope="scope-dev", key="secret-sql-user-conection")
password=dbutils.secrets.get(scope="scope-dev", key="secret-sql-password-conection")
server=dbutils.secrets.get(scope="scope-dev", key="secret-sql-server-conetion")
database="aprovibdapp"

# COMMAND ----------

jdbc_url = (
    f"jdbc:sqlserver://{server}:1433;"
    f"database={database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

# COMMAND ----------

connection_properties = {
    "user": user,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

table = "dbo.transacciones"
df_sql = spark.read \
    .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
display(df_sql.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col

datos_formateados = df_sql.select(
    col("id_trx").cast("int"),
    col("id_usuario"),
    col("tipo_operacion"),
    col("monto").cast("decimal(18,2)"),
    col("fecha"),
    col("hora")
).orderBy("fecha")

# COMMAND ----------

# 1. Definimos la ruta del  storage (External Location)
path = "abfss://bronze@aprovidatalake.dfs.core.windows.net/"

# 2. Definimos la carpeta para la tabla
folder = "datos_estructurados"
full_path = f"{path}{folder}"

# 3. Escribimos en formato Parquet
datos_formateados.write \
    .mode("overwrite") \
    .parquet(full_path)

print(f"Escritura completada en: {full_path}")

# COMMAND ----------

# validamos carga de datos en capa bronce
dbutils.fs.ls("abfss://bronze@aprovidatalake.dfs.core.windows.net/datos_estructurados")
