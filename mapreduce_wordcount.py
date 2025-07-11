import os
import multiprocessing
from collections import Counter
import time

 
# Reading the entire file and split into chunks
def read_chunks(file_path, num_chunks):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    chunk_size = len(lines) // num_chunks
    return [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]


# MAP step: Count words in a chunk
def count_words(chunk):
    counter = Counter()
    for line in chunk:
        words = line.strip().lower().split()
        counter.update(words)
    return counter


# REDUCE step: Combine all Counter results
def reduce_counters(counters):
    total = Counter()
    for c in counters:
        total.update(c)
    return total


if __name__ == "__main__":
    start_time = time.time()
    
    # Updating this with your dataset path
    input_file = "Hotel_Reviews.csv"

    # Number of parallel processes to use
    num_processes = multiprocessing.cpu_count()

    print(f"Using {num_processes} parallel processes.")

    # Reading chunks
    chunks = read_chunks(input_file, num_processes)

    # Parallel processing
    with multiprocessing.Pool(num_processes) as pool:
        partial_counts = pool.map(count_words, chunks)

    # Reduceing step
    final_counts = reduce_counters(partial_counts)

    # Displaying top 10 words
    print("\nTop 10 most frequent words:")
    for word, count in final_counts.most_common(10):
        print(f"{word}: {count}")

    print(f"\nCompleted in {time.time() - start_time:.2f} seconds.")
