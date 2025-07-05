from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaHotelReviewWordCount") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hotel-reviews") \
    .load()

# Convert binary to string
lines = df.selectExpr("CAST(value AS STRING) as text")

# Word Count Logic
words = lines.select(explode(split(col("text"), " ")).alias("word")) \
             .groupBy("word") \
             .count()

# Write output to console
query = words.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

