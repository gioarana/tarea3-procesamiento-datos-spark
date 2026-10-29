from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName("OnlineRetailEDA") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

ruta_limpia = "/home/gio/tarea3/resultados/ventas_limpias.parquet"

df = spark.read.parquet(ruta_limpia)

print("\n--- Estadísticas generales ---")
df.select("Quantity", "UnitPrice", "TotalSales") \
    .summary("count", "mean", "min", "25%", "50%", "75%", "max") \
    .show()

# Ventas por país
ventas_pais = df.groupBy("Country") \
    .agg(
        F.round(F.sum("TotalSales"), 2).alias("VentasTotales"),
        F.countDistinct("InvoiceNo").alias("NumeroFacturas")
    ) \
    .orderBy(F.desc("VentasTotales"))

print("\n--- Diez países con mayores ventas ---")
ventas_pais.show(10, truncate=False)

# Productos con mayores ingresos
productos_top = df.groupBy("Description") \
    .agg(
        F.sum("Quantity").alias("UnidadesVendidas"),
        F.round(F.sum("TotalSales"), 2).alias("VentasTotales")
    ) \
    .orderBy(F.desc("VentasTotales"))

print("\n--- Diez productos con mayores ventas ---")
productos_top.show(10, truncate=False)

# Ventas por mes
ventas_mes = df.withColumn(
    "Mes",
    F.date_format(F.col("InvoiceDateTS"), "yyyy-MM")
).groupBy("Mes") \
    .agg(
        F.round(F.sum("TotalSales"), 2).alias("VentasTotales"),
        F.countDistinct("InvoiceNo").alias("NumeroFacturas")
    ) \
    .orderBy("Mes")

print("\n--- Ventas por mes ---")
ventas_mes.show(20, truncate=False)

# Clientes con mayor valor de compra
clientes_top = df.filter(F.col("CustomerID").isNotNull()) \
    .groupBy("CustomerID") \
    .agg(
        F.countDistinct("InvoiceNo").alias("NumeroFacturas"),
        F.round(F.sum("TotalSales"), 2).alias("VentasTotales")
    ) \
    .orderBy(F.desc("VentasTotales"))

print("\n--- Diez clientes con mayor valor de compra ---")
clientes_top.show(10, truncate=False)

# Exportar resultados para las visualizaciones posteriores
ventas_pais.coalesce(1) \
    .write.mode("overwrite").option("header", "true") \
    .csv("/home/gio/tarea3/resultados/eda/ventas_por_pais")

productos_top.limit(10).coalesce(1) \
    .write.mode("overwrite").option("header", "true") \
    .csv("/home/gio/tarea3/resultados/eda/productos_top_10")

ventas_mes.coalesce(1) \
    .write.mode("overwrite").option("header", "true") \
    .csv("/home/gio/tarea3/resultados/eda/ventas_por_mes")

clientes_top.limit(10).coalesce(1) \
    .write.mode("overwrite").option("header", "true") \
    .csv("/home/gio/tarea3/resultados/eda/clientes_top_10")

spark.stop()
