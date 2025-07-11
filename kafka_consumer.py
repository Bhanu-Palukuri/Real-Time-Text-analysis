from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType

# Spark Session
spark = SparkSession.builder \
    .appName("KafkaHotelReviewConsumer") \
    .getOrCreate()

# Reading from Kafka topic
df = spark.readStream \ 
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hotel-reviews") \
    .option("startingOffsets", "latest") \
    .load()

# Defining schema of the incoming JSON data
schema = StructType() \
    .add("hotel", StringType()) \
    .add("positive", StringType()) \
    .add("negative", StringType()) \
    .add("score", FloatType())

# Converting Kafka 'value' from bytes to string and parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select("data.*")

#  count by hotel name
counts = json_df.groupBy("hotel").count()

# Output to console
query = counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
