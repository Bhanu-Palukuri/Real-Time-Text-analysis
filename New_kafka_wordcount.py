from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder \
    .appName("KafkaHotelReviewWordCount") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hotel-reviews") \
    .load()

lines = df.selectExpr("CAST(value AS STRING) as text")

words = lines.select(explode(split(col("text"), " ")).alias("word")) \
             .groupBy("word").count()

query = words.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

