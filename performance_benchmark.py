import pandas as pd
import re
import time
import matplotlib.pyplot as plt
from collections import Counter
from multiprocessing import Pool, cpu_count

# Loading dataset
df = pd.read_csv("Hotel_Reviews.csv")
texts = (
    df['Positive_Review'].fillna('') + " " + df['Negative_Review'].fillna('')
).astype(str).tolist()

# Tokenizer
def tokenize(text):
    return re.findall(r'\w+', text.lower())

# Sequential Word Count
def sequential_wordcount(texts):
    total_words = Counter()
    start = time.time()
    for text in texts:
        total_words.update(tokenize(text))
    end = time.time()
    return end - start  # latency

# Parallel Word Count
def mp_wordcount(text_chunk):
    local_counter = Counter()
    for text in text_chunk:
        local_counter.update(tokenize(text))
    return local_counter

def parallel_wordcount(texts):
    num_workers = cpu_count()
    chunk_size = len(texts) // num_workers
    chunks = [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]
    
    start = time.time()
    with Pool(processes=num_workers) as pool:
        counters = pool.map(mp_wordcount, chunks)
    total = Counter()
    for c in counters:
        total.update(c)
    end = time.time()
    return end - start  # latency

# Benchmarking
sample_sizes = [100, 1000, 5000, 10000, 20000]
seq_latencies = []
par_latencies = []
seq_throughput = []
par_throughput = []

for size in sample_sizes:
    sample_texts = texts[:size]

    seq_time = sequential_wordcount(sample_texts)
    par_time = parallel_wordcount(sample_texts)

    seq_latencies.append(seq_time)
    par_latencies.append(par_time)

    seq_throughput.append(size / seq_time)
    par_throughput.append(size / par_time)

# Plotting Latency
plt.figure(figsize=(10, 5))
plt.plot(sample_sizes, seq_latencies, marker='o', label='Sequential')
plt.plot(sample_sizes, par_latencies, marker='o', label='Parallel')
plt.title("Latency Comparison")
plt.xlabel("Number of Reviews")
plt.ylabel("Latency (Seconds)")
plt.legend()
plt.grid(True)
plt.savefig("latency_comparison.png")
plt.show()

# Plotting Throughput
plt.figure(figsize=(10, 5))
plt.plot(sample_sizes, seq_throughput, marker='s', label='Sequential')
plt.plot(sample_sizes, par_throughput, marker='s', label='Parallel')
plt.title("Throughput Comparison")
plt.xlabel("Number of Reviews")
plt.ylabel("Throughput (Records/sec)")
plt.legend()
plt.grid(True)
plt.savefig("throughput_comparison.png")
plt.show()
