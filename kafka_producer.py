from kafka import KafkaProducer
import json
import pandas as pd
import time

# Loading hotel reviews
hotel_df = pd.read_csv('Hotel_Reviews.csv').fillna('') 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(" Streaming Hotel Reviews to Kafka...")
for _, row in hotel_df.iterrows():
    data = {
        'hotel': row['Hotel_Name'],
        'positive': row['Positive_Review'],
        'negative': row['Negative_Review'],
        'score': row['Reviewer_Score']
    }
    producer.send('hotel-reviews', value=data)
    print("Sent:", data['hotel'])
    time.sleep(0.1)

producer.flush()
print(" Done.")
