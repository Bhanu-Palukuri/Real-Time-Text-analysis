from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window
from pyspark.sql.types import StringType

# Create Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaStreamWordCount") \
    .getOrCreate() 

spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hotel_reviews") \
    .option("startingOffsets", "latest") \
    .load()

# Extract value (as string)
lines = df.selectExpr("CAST(value AS STRING) as review")

# Split into words
words = lines.select(explode(split(lines.review, " ")).alias("word"))

# Remove empty words, lowercase
filtered = words.filter("length(word) > 3").selectExpr("lower(word) as word")

# Sliding window word count (5-min window, sliding every 1 min)
windowed_counts = filtered \
    .groupBy(
        window(filtered.timestamp, "5 minutes", "1 minute"),
        filtered.word
    ).count() \
    .orderBy("window", "count", ascending=False)

# Start query - write to console for now
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()

query.awaitTermination()
