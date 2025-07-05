import time
import pandas as pd
import re
from collections import Counter
from multiprocessing import Pool, cpu_count

# Loading and Combine Review Columns
df = pd.read_csv("Hotel_Reviews.csv")

# Combining positive and negative reviews into one string per row
texts = (
    df['Positive_Review'].fillna('') + " " + df['Negative_Review'].fillna('')
).astype(str).tolist()

# Tokenizer
def tokenize(text):
    return re.findall(r'\w+', text.lower())

# Sequential Word Count
def sequential_wordcount(texts):
    start = time.time()
    total_words = Counter()
    for text in texts:
        total_words.update(tokenize(text))
    duration = time.time() - start
    return total_words, duration

# Parallel Word Count
def mp_wordcount(text_chunk):
    local_counter = Counter()
    for text in text_chunk:
        local_counter.update(tokenize(text))
    return local_counter

def parallel_wordcount(texts):
    start = time.time()
    num_workers = cpu_count()
    chunk_size = len(texts) // num_workers
    chunks = [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]

    with Pool(processes=num_workers) as pool:
        counters = pool.map(mp_wordcount, chunks)

    total = Counter()
    for c in counters:
        total.update(c)

    duration = time.time() - start
    return total, duration

# Main
if __name__ == "__main__":
    print(" Benchmarking Word Count on Hotel_Reviews.csv")

    # Sequential processing
    seq_counts, seq_time = sequential_wordcount(texts)
    print(f"\n Sequential Time: {seq_time:.2f} sec")
    print(" Top 5 Words (Sequential):", seq_counts.most_common(5))

    # Parallel processing
    mp_counts, mp_time = parallel_wordcount(texts)
    print(f"\nParallel Time (Multiprocessing): {mp_time:.2f} sec")
    print(" Top 5 Words (Parallel):", mp_counts.most_common(5))

