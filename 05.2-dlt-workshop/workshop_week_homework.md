# Homework: Build Your Own dlt Pipeline

### Question 1: What is the start date and end date of the dataset?

- 2009-01-01 to 2009-01-31
- 2009-06-01 to 2009-07-01 ✅
- 2024-01-01 to 2024-02-01
- 2024-06-01 to 2024-07-01

```sql
SELECT
  MIN(CAST(trip_pickup_date_time AS DATE)) AS start_date,
  MAX(CAST(trip_pickup_date_time AS DATE)) AS end_date
FROM taxi_data;
```

### Question 2: What proportion of trips are paid with credit card?

- 16.66%
- 26.66% ✅
- 36.66%
- 46.66%

```sql
WITH stats AS (
  SELECT
    COUNT(*) AS total_trips,
    SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) AS credit_trips
  FROM taxi_data
)
SELECT
  100.0 * credit_trips / total_trips AS credit_percentage
FROM stats;
```

### Question 3: What is the total amount of money generated in tips?

- $4,063.41
- $6,063.41 ✅
- $8,063.41
- $10,063.41

```sql
SELECT
  SUM(tip_amt) AS total_tips
FROM taxi_data;
```