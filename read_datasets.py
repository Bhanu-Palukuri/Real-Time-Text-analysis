import pandas as pd

# Hoteling Reviews (CSV)
hotel_df = pd.read_csv('Hotel_Reviews.csv')
print("\n Hotel Reviews Sample:")
print(hotel_df.head(3))

# Amazon Electronics Reviews (TSV)
amazon_df = pd.read_csv(
    'amazon_reviews_us_Electronics_v1_00.tsv',
    sep='\t',
    on_bad_lines='skip',   
    low_memory=False
)
print("\n Amazon Reviews Sample:")
print(amazon_df.head(3))

