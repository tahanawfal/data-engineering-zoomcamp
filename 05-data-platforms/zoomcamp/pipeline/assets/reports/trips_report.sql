/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Asset: reports.trips_report
name: reports.trips_report
# Platform type for local development
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: taxi_type
    type: STRING
    description: Taxi service type (yellow/green)
    primary_key: true
  - name: report_date
    type: DATE
    description: Date of the report
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  COALESCE(taxi_type, 'unknown') AS taxi_type,
  CAST(pickup_datetime AS DATE) AS report_date,
  COUNT(*) AS trip_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1,2
