from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Lectura continua desde Kafka
datos_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Conversión de JSON a columnas estructuradas
datos_sensor = datos_kafka \
    .select(
        F.from_json(
            F.col("value").cast("string"),
            schema
        ).alias("data")
    ) \
    .select("data.*") \
    .withColumn(
        "timestamp_evento",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

# Estadísticas por sensor en ventanas de un minuto
estadisticas = datos_sensor \
    .groupBy(
        F.window(F.col("timestamp_evento"), "1 minute"),
        F.col("sensor_id")
    ) \
    .agg(
        F.round(F.avg("temperature"), 2).alias("TemperaturaPromedio"),
        F.round(F.avg("humidity"), 2).alias("HumedadPromedio"),
        F.count("*").alias("NumeroEventos")
    ) \
    .select(
        F.col("window.start").alias("InicioVentana"),
        F.col("window.end").alias("FinVentana"),
        "sensor_id",
        "TemperaturaPromedio",
        "HumedadPromedio",
        "NumeroEventos"
    )

consulta = estadisticas.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option(
        "checkpointLocation",
        "/home/gio/tarea3/resultados/checkpoint_sensor_data"
    ) \
    .start()

consulta.awaitTermination()
