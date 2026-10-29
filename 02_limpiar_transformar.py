from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName("OnlineRetailLimpieza") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

ruta_archivo = "/home/gio/tarea3/data/online_retail.csv"

df = spark.read \
    .option("header", "true") \
    .option("sep", ";") \
    .option("encoding", "UTF-8") \
    .option("inferSchema", "true") \
    .csv(ruta_archivo)

# Limpieza y transformación de columnas
df_transformado = df \
    .withColumn("Description", F.trim(F.col("Description"))) \
    .withColumn(
        "UnitPrice",
        F.regexp_replace(F.col("UnitPrice"), ",", ".").cast("double")
    ) \
    .withColumn(
        "InvoiceDateTS",
        F.to_timestamp(F.col("InvoiceDate"), "d/M/yy H:mm")
    ) \
    .withColumn(
        "IsCancellation",
        F.col("InvoiceNo").startswith("C")
    ) \
    .withColumn(
        "TotalSales",
        F.round(F.col("Quantity") * F.col("UnitPrice"), 2)
    )

print("\n--- Calidad inicial de los datos ---")

df_transformado.select(
    F.count("*").alias("Total_registros"),
    F.sum(F.when(F.col("Description").isNull(), 1).otherwise(0)).alias("Descripcion_nula"),
    F.sum(F.when(F.col("CustomerID").isNull(), 1).otherwise(0)).alias("Cliente_nulo"),
    F.sum(F.when(F.col("UnitPrice").isNull(), 1).otherwise(0)).alias("Precio_nulo"),
    F.sum(F.when(F.col("IsCancellation"), 1).otherwise(0)).alias("Cancelaciones")
).show()

# Solo se conservan ventas válidas para el análisis comercial
ventas_validas = df_transformado.filter(
    F.col("Description").isNotNull() &
    F.col("InvoiceDateTS").isNotNull() &
    (F.col("Quantity") > 0) &
    (F.col("UnitPrice") > 0) &
    (~F.col("IsCancellation"))
)

print("\n--- Esquema después de la transformación ---")
ventas_validas.printSchema()

print("\n--- Muestra de ventas válidas ---")
ventas_validas.select(
    "InvoiceNo",
    "Description",
    "Quantity",
    "UnitPrice",
    "InvoiceDateTS",
    "CustomerID",
    "Country",
    "TotalSales"
).show(10, truncate=False)

print("\n--- Cantidad de ventas válidas ---")
print(ventas_validas.count())

# Guardar el resultado limpio en formato Parquet
ventas_validas.write \
    .mode("overwrite") \
    .parquet("/home/gio/tarea3/resultados/ventas_limpias.parquet")

spark.stop()

