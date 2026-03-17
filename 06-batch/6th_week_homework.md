# Module 6 Homework

## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

```py
# Initialize uv and add pyspark
uv init
uv add pyspark

import pyspark
from pyspark.sql import SparkSession

# Initialize a local Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("SparkVersion") \
    .getOrCreate()

spark.version
```
`4.1.1`

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB ✅
- 75MB
- 100MB

### Ingest Files into local files
```py
import pyspark
from pyspark.sql import SparkSession

# Initialize session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PartitionSize") \
    .getOrCreate()

# Read the original parquet file
df = spark.read.parquet('yellow_tripdata_2025-11.parquet')

# Repartition and save
output_path = 'data/pq/yellow/2025/11/'
df.repartition(4).write.parquet(output_path, mode='overwrite')

df = df.repartition(4)
```

### Find out the Average Size of files
```py
import os

folder = "data/pq/yellow/2025/11/"
files = [f for f in os.listdir(folder) if f.endswith(".parquet")]

sizes = [os.path.getsize(os.path.join(folder, f)) for f in files]
avg_mb = sum(sizes) / len(sizes) / (1024 * 1024)

print(f"Files: {len(files)}")
for f, s in zip(files, sizes):
    print(f"  {f}: {s/(1024*1024):.2f} MB")
print(f"\nAvg Size: {avg_mb:.2f} MB")
```

### Output of excuting code
```
Files: 4
  part-00000-763ca6fa-08ab-4203-bf2d-5513339877e2-c000.snappy.parquet: 25.34 MB
  part-00001-763ca6fa-08ab-4203-bf2d-5513339877e2-c000.snappy.parquet: 25.32 MB
  part-00002-763ca6fa-08ab-4203-bf2d-5513339877e2-c000.snappy.parquet: 25.32 MB
  part-00003-763ca6fa-08ab-4203-bf2d-5513339877e2-c000.snappy.parquet: 25.34 MB
Avg Size: 25.33 MB
```

## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- 162,604 ✅
- 225,768

```py
from pyspark.sql import functions as F

trips_nov_15 = df_trips.filter(
    F.to_date(df_trips.tpep_pickup_datetime) == '2025-11-15'
).count()

print(f"Total trips on November 15, 2025: {trips_nov_15}")
```
162604

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- 90.6 ✅
- 134.5

```py
from pyspark.sql import functions as F

# Calculate duration in hours
df_with_duration = df_trips.withColumn(
    'duration_hours', 
    (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600
)

# Get the maximum duration
longest_trip = df_with_duration.select(F.max('duration_hours')).first()[0]
print(f"Longest trip in hours: {longest_trip}")
```
90.6

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040 ✅
- 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island ✅
- Arden Heights ✅
- Rikers Island
- Jamaica Bay

```py
# Load the zone lookup data
df_zones = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('/data/raw/taxi_zone_lookup.csv')

# Create temporary views
df_zones.createOrReplaceTempView("zones")
df_trips.createOrReplaceTempView("trips")

# Query to find the least frequent pickup location zone
spark.sql("""
    SELECT 
        z.Zone, 
        COUNT(1) AS pickup_count
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY pickup_count ASC
    LIMIT 5
""").show(truncate=False)
```

```
+---------------------------------------------+-----+
|Zone                                         |trips|
+---------------------------------------------+-----+
|Governor's Island/Ellis Island/Liberty Island|1    |
|Eltingville/Annadale/Prince's Bay            |1    |
|Arden Heights                                |1    |
|Port Richmond                                |3    |
|Rikers Island                                |4    |
+---------------------------------------------+-----+
```