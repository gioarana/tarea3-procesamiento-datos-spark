# Procesamiento de datos con Apache Spark

Proyecto académico de la **Tarea 3 – Procesamiento de Datos con Apache Spark**. El proyecto combina análisis por lotes de ventas minoristas con procesamiento de eventos en tiempo real mediante Apache Kafka y Spark Structured Streaming.

## Contenido

| Archivo | Propósito |
| --- | --- |
| `01_cargar_datos.py` | Lee el archivo CSV y muestra el esquema, una muestra y el total de registros. |
| `02_limpiar_transformar.py` | Limpia los datos, transforma tipos, identifica cancelaciones y guarda las ventas válidas en Parquet. |
| `03_analisis_exploratorio.py` | Calcula ventas por país, producto, mes y cliente; además exporta resultados para visualización. |
| `04_visualizaciones.py` | Genera las gráficas del análisis exploratorio con Matplotlib. |
| `kafka_producer.py` | Simula y publica eventos de sensores en el tópico Kafka `sensor_data`. |
| `spark_streaming_consumer.py` | Consume el tópico con Spark Structured Streaming y calcula métricas por ventana de un minuto. |

## Fuente de datos

El análisis batch utiliza el conjunto **Online Retail**, disponible en el repositorio oficial de UCI Machine Learning Repository:

> Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33

El archivo original `Online Retail.xlsx` debe convertirse a CSV con separador `;`, decimal `,` y guardarse como `online_retail.csv`. El conjunto no se incluye en el repositorio para mantener el proyecto ligero y respetar la distribución original de UCI.

## Requisitos

- Ubuntu Server 22.04 o una distribución Linux equivalente.
- Java 11.
- Apache Spark 3.5.8.
- Apache Kafka 3.9.2 y un broker activo en `localhost:9092`.
- Python 3, `pyspark`, `kafka-python` y `matplotlib`.

Instalación de las dependencias de Python:

```bash
python3 -m pip install -r requirements.txt
```

## Estructura esperada en Ubuntu

Los scripts fueron ejecutados con el usuario `gio`; por ello usan rutas absolutas bajo `/home/gio/tarea3`.

```text
/home/gio/tarea3/
├── data/
│   └── online_retail.csv
├── resultados/
│   ├── eda/
│   └── graficas/
└── scripts/
```

Si se usa otro usuario o ubicación, se deben ajustar las variables de ruta al inicio de los scripts.

## Ejecución del análisis batch

Desde `/home/gio/tarea3/scripts`:

```bash
spark-submit 01_cargar_datos.py
spark-submit 02_limpiar_transformar.py
spark-submit 03_analisis_exploratorio.py
python3 04_visualizaciones.py
```

El segundo script guarda el resultado limpio en `resultados/ventas_limpias.parquet`; se usa el modo `overwrite`, por lo que una nueva ejecución reemplaza el resultado previo.

## Ejecución de streaming

Con Kafka activo, crear el tópico si aún no existe:

```bash
/opt/Kafka/bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

En una terminal, iniciar el consumidor de Spark:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  spark_streaming_consumer.py
```

En una segunda terminal, iniciar el productor:

```bash
python3 kafka_producer.py
```

El consumidor presenta por consola la ventana de tiempo, el identificador del sensor, los promedios de temperatura y humedad, y el número de eventos recibidos.

## Video de sustentación

La demostración de la ejecución está disponible en: https://youtu.be/qyXLZOgsldk

