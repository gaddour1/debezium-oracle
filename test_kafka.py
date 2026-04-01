from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

spark = SparkSession.builder \
    .appName("Test-T24-Kafka") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "oracle-cdc-test.C__DBZUSER.ACCOUNT_TEST") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df.select(
    get_json_object(col("value").cast("string"), "$.payload.op").alias("op"),
    get_json_object(col("value").cast("string"), "$.payload.after.NAME").alias("NAME"),
    col("timestamp").alias("kafka_ts")
)

df_parsed.show(10, truncate=False)
print(f"Total messages : {df_parsed.count()}")

spark.stop()