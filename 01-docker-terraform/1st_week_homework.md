# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.


## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

- 25.3 ✅
- 24.3.1
- 24.2.1
- 23.3.1

To check the `pip` version inside the `python:3.13` Docker image, run:
```bash
docker run python:3.13 sh -c "pip --version"
```


## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?

```yaml
services:
  pgdatabase:
    container_name: pgdatabase
    image: postgres:18
    networks:
      - pg-network
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5432:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    networks:
      - pg-network
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
  vol-pgadmin_data:

networks:
  pg-network:
    name: pg-network
    driver: bridge
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432 ✅

If multiple answers are correct, select any 


## Prepare the Data

### Download the green taxi trips data for November 2025:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

---

### Start services

```bash
docker compose up -d
```

---

### Load data into PostgreSQL

Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Create virtual environment and install dependencies

```bash
uv venv
source .venv/bin/activate
uv pip install pandas pyarrow sqlalchemy psycopg2-binary
```

Create `load_data.py`

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5433/ny_taxi"
)

pd.read_parquet("green_tripdata_2025-11.parquet") \
  .to_sql("green_tripdata_2025_11", engine, if_exists="replace", index=False)

pd.read_csv("taxi_zone_lookup.csv") \
  .to_sql("taxi_zone_lookup", engine, if_exists="replace", index=False)
```

Run `load_data.py`

```bash
uv run load_data.py
```

---

## Run SQL

**pgAdmin**
- URL: `http://localhost:8080`
- Host: `db`
- Port: `5432`
- Database: `ny_taxi`
- User: `postgres`
- Password: `postgres`

---

## Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

- 7,853
- 8,007 ✅
- 8,254
- 8,421

```sql
SELECT COUNT(*)
FROM green_tripdata_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;
```


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

Use the pick up time for your calculations.

- 2025-11-14 ✅
- 2025-11-20
- 2025-11-23
- 2025-11-25

```sql
SELECT
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS max_trip_distance
FROM green_tripdata_2025_11
WHERE
    trip_distance < 100
    AND DATE(lpep_pickup_datetime) IN (
        DATE '2025-11-14',
        DATE '2025-11-20',
        DATE '2025-11-23',
        DATE '2025-11-25'
    )
GROUP BY pickup_date
ORDER BY max_trip_distance DESC
LIMIT 1;
```


## Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

- East Harlem North ✅
- East Harlem South
- Morningside Heights
- Forest Hills

```sql
SELECT
    z."Zone" AS pickup_zone,
    SUM(g.total_amount) AS total_amount_sum
FROM green_tripdata_2025_11 g
JOIN taxi_zone_lookup z
    ON g."PULocationID" = z."LocationID"
WHERE DATE(g.lpep_pickup_datetime) = '2025-11-18'
  AND z."Zone" IN (
      'East Harlem North',
      'East Harlem South',
      'Morningside Heights',
      'Forest Hills'
  )
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;
```


## Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

- JFK Airport
- Yorkville West ✅
- East Harlem North
- LaGuardia Airport

```sql
SELECT
    z_do."Zone" AS dropoff_zone,
    SUM(g.tip_amount) AS total_tip
FROM green_tripdata_2025_11 g
JOIN taxi_zone_lookup z_pu
    ON g."PULocationID" = z_pu."LocationID"
JOIN taxi_zone_lookup z_do
    ON g."DOLocationID" = z_do."LocationID"
WHERE z_pu."Zone" = 'East Harlem North'
  AND z_do."Zone" IN (
      'JFK Airport',
      'Yorkville West',
      'East Harlem North',
      'LaGuardia Airport'
  )
GROUP BY z_do."Zone"
ORDER BY total_tip DESC
LIMIT 1;
```


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform.
Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy ✅
- terraform import, terraform apply -y, terraform rm
