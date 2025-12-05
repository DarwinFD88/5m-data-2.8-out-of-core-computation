# Assignment

## Brief

Write the Python codes for the following questions.

## Instructions

Paste the answer as Python in the answer code section below each question.

### Question 1

Question: Join the `metadata_pl` and `ratings_pl` DataFrames in Polars, then calculate the average rating by `genre` and by `original_language` (separately).

Answer:

```python
#Join both DataFrames
joined_df = metadata_pl.join(ratings_pl, left_on='id', right_on='movieId', how='inner')

#Average rating by genre
avg_ratings_by_genre = (joined_df.lazy()
                        .groupby('genres')
                        .agg(pl.col('rating').mean().alias('average_rating'))
                        .sort('average_rating', descending=True))
avg_ratings_by_genre.collect()

#Average rating by original language
avg_ratings_by_original_language = (joined_df.lazy()
                                    .groupby('original_language')
                                    .agg(pl.col('rating').mean().alias('average_rating'))
                                    .sort('average_rating', descending=True))
avg_ratings_by_original_language.collect()

```

### Question 2

Question: Calculate the average total amount for vendors with at least 5 trips from the NYC Taxi Trip Data.

Answer:

```python
q2 = (pl.scan_csv('data/taxi_trip_data.csv')
      .groupby('vendor_id')
      .agg([pl.count('total_amount').alias('total_trips'),
            pl.mean('total_amount').alias('average_amount')])
      .filter(pl.col('total_trips') >= 5)
      .sort('average_amount', descending=True)
      )

q2.collect(streaming=True)
```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
