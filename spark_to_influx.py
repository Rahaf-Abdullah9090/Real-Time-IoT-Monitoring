from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os

# ---- Influx Config ----
INFLUX_URL    = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN", "admintoken")
INFLUX_ORG    = os.getenv("INFLUX_ORG", "myorg")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "iot_data")

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api()

# ---- Spark Session ----
spark = SparkSession.builder.appName("KafkaToInflux").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---- Schema (MATCHES YOUR MQTT → Kafka messages!) ----
schema = StructType([
    StructField("device_id", StringType()),
    StructField("V_L1", DoubleType()),
    StructField("I_L1", DoubleType()),
    StructField("VA_L1", DoubleType()),
    StructField("P_L1", DoubleType()),
    StructField("timestamp", LongType())
])

# ---- Read Kafka ----
df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka:9092")
         .option("subscribe", "sensor-data")  # <-- Your topic
         .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("d"))
      .select("d.*")
)

# ---- Write Batch to InfluxDB ----
def write_batch(batch_df, batch_id):

    points = []

    for row in batch_df.toLocalIterator():

        # ----------------------------
        # ⭐ NEW: Compute Power Factor
        # ----------------------------
        try:
            power_factor = row.P_L1 / row.VA_L1 if row.VA_L1 not in (None, 0) else 0
        except:
            power_factor = 0
        # ----------------------------

        # Build InfluxDB point
        points.append(
            Point("modbus_metrics")
                .tag("device_id", row.device_id)
                .field("V_L1", row.V_L1)
                .field("I_L1", row.I_L1)
                .field("VA_L1", row.VA_L1)
                .field("P_L1", row.P_L1)
                .field("PF_L1", power_factor)     # ⭐ NEW FIELD ADDED
                .time(row.timestamp, WritePrecision.S)
        )

    # Write to InfluxDB
    if points:
        print(f"Writing {len(points)} points to InfluxDB…")
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

# Start streaming
parsed.writeStream.foreachBatch(write_batch).start().awaitTermination()
