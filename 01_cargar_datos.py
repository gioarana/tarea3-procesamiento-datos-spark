from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OnlineRetailBatch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

ruta_archivo = "/home/gio/tarea3/data/online_retail.csv"

df = spark.read \
    .option("header", "true") \
    .option("sep", ";") \
    .option("decimal", ",") \
    .option("encoding", "UTF-8") \
    .option("inferSchema", "true") \
    .csv(ruta_archivo)

print("\n--- Esquema del DataFrame ---")
df.printSchema()

print("\n--- Primeros cinco registros ---")
df.show(5, truncate=False)

print("\n--- Cantidad total de registros ---")
print(df.count())

spark.stop()
