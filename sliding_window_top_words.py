from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col, lower
from pyspark.sql.types import StringType

# Initializing Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamTopWords") \
    .getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")

# Reading from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hotel-reviews") \
    .option("startingOffsets", "latest") \
    .load()

# Extracting value and timestamp
text_df = df.selectExpr("CAST(value AS STRING)", "timestamp")

# Tokenization and cleaning
words = text_df.select(
    explode(split(lower(col("value")), "\\W+")).alias("word"),
    col("timestamp")
).filter("word != ''")

# Aggregate word counts over sliding window
windowed_counts = words.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute"),
    col("word")
).count()

# Result
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .start()

query.awaitTermination()

