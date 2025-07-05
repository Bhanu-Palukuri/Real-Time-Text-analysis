# Real-Time Parallel Text Analytics System using Kafka, Spark, and AWS EC2

This project implements a distributed real-time text analytics pipeline using **Apache Kafka**, **Apache Spark Streaming**, and **AWS EC2** instances. It processes hotel review data with parallelism and benchmarks sequential vs. parallel performance.

---

##  Phase 1: Design & Setup

###  Problem Statement
We aim to process a large volume of hotel reviews in real-time to extract insights like keyword trends, sentiment, and word counts. The system must support scalability and high-throughput using parallel processing.

###  Why Parallel Processing?
Processing text data like sentiment analysis or word count involves CPU-intensive tasks that benefit greatly from parallelism. Using Spark Streaming with Kafka allows us to scale horizontally across EC2 instances.

---

##  AWS Setup & Architecture

### Services Used
- **Amazon EC2**: Hosts Spark, Kafka, and Python processing scripts.
- **Amazon S3**: Store intermediate and final outputs.
- **CloudWatch**: Monitor Spark jobs and EC2 health.
- **Auto Scaling Group**: To enable scaling based on CPU load.

### Architecture Diagram
![Architecture_diagram](https://github.com/user-attachments/assets/fd337308-0de9-4cf9-bfa0-119cb9b7b684)


###  1. Data Ingestion with Kafka
- `kafka_producer.py`: Reads hotel reviews and pushes messages into the Kafka topic `hotel-reviews`.

  ```bash
  python3 kafka_producer.py
  ```
### 2. Real-Time Stream Processing (Filtering, Counting)
- kafka_consumer.py: Consumes streaming data from Kafka and processes it in Spark Streaming.

```bash
spark-submit \
  --jars "$HOME/spark-jars/*.jar" \
  kafka_consumer.py

```
- sliding_window_top_words.py: Performs real-time sliding window word counts using Spark Structured Streaming.

```bash
spark-submit \
  --jars "$HOME/spark-jars/*.jar" \
  sliding_window_top_words.py

```
# Phase 2
###  MapReduce: Batch Word Count + Sentiment Analysis
- Implemented using Python multiprocessing.
- File: hybrid_wordcount_benchmark.py

```
python3 hybrid_wordcount_benchmark.py
```
###  Hybrid Parallelism
Combines:
- Data parallelism via multiprocessing
- Task parallelism by running the jobs on EC2 instances

# Phase 3: Performance Benchmarking
Benchmarks run using multiple data sizes

```
Requirements
Python 3.12+

Apache Spark 3.5+

Apache Kafka 3.5+

Java 21

pandas, matplotlib

multiprocessing (built-in)
```
