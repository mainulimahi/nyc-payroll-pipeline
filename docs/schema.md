# Database Schema

## Overview

The pipeline uses two schemas inside the `nyc_payroll_db` database:

| Schema | Purpose |
|---|---|
| `staging` | Raw data as received from the NYC Open Data API (landing zone) |
| `public` | Cleaned, deduplicated, and transformed data with load timestamps (historical layer) |

---

## Data Pattern

| Table | Pattern | Description |
|---|---|---|
| `staging.nyc_payroll` | **Full refresh** | Truncated and reloaded on every pipeline run |
| `public.nyc_payroll` | **Append** | New batch appended on every run, tagged with `loaded_at` timestamp |

---

## `staging.nyc_payroll`

Raw payroll records inserted directly from the NYC Open Data API with minimal processing (type casting and negative value filtering only).

| Column | Type | Description |
|---|---|---|
| `fiscal_year` | INT | NYC fiscal year (e.g. 2025) |
| `payroll_number` | INT | Agency payroll number |
| `agency_name` | VARCHAR(200) | Name of the NYC agency (raw, uppercase) |
| `last_name` | VARCHAR(100) | Employee last name (raw, uppercase) |
| `first_name` | VARCHAR(100) | Employee first name (raw, uppercase) |
| `mid_init` | VARCHAR(10) | Middle initial |
| `agency_start_date` | DATE | Date employee joined the agency |
| `work_location_borough` | VARCHAR(100) | Borough of work location (raw, uppercase) |
| `title_description` | VARCHAR(200) | Job title (raw, uppercase) |
| `leave_status_as_of_june_30` | VARCHAR(50) | Employment status at fiscal year end |
| `base_salary` | FLOAT | Annual or daily base salary |
| `pay_basis` | VARCHAR(50) | Pay frequency (e.g. `per Annum`, `per Day`) |
| `regular_hours` | FLOAT | Regular hours worked in fiscal year |
| `regular_gross_paid` | FLOAT | Gross pay for regular hours |
| `ot_hours` | FLOAT | Overtime hours worked |
| `total_ot_paid` | FLOAT | Total overtime pay |
| `total_other_pay` | FLOAT | Other compensation (bonuses, allowances, etc.) |

**Row count:** ~981 rows per run (after negative value filtering from 1000 API rows)

---

## `public.nyc_payroll`

Cleaned and transformed version of staging data. Each pipeline run appends a new batch tagged with the current timestamp — enabling historical tracking.

| Column | Type | Description |
|---|---|---|
| `fiscal_year` | INT | NYC fiscal year |
| `payroll_number` | INT | Agency payroll number |
| `agency_name` | VARCHAR(200) | Agency name (Title Case) |
| `last_name` | VARCHAR(100) | Employee last name (Title Case) |
| `first_name` | VARCHAR(100) | Employee first name (Title Case) |
| `mid_init` | VARCHAR(10) | Middle initial |
| `agency_start_date` | DATE | Date employee joined the agency |
| `work_location_borough` | VARCHAR(100) | Borough (Title Case) |
| `title_description` | VARCHAR(200) | Job title (Title Case) |
| `leave_status_as_of_june_30` | VARCHAR(50) | Employment status (Title Case) |
| `base_salary` | FLOAT | Annual or daily base salary |
| `pay_basis` | VARCHAR(50) | Pay frequency |
| `regular_hours` | FLOAT | Regular hours worked |
| `regular_gross_paid` | FLOAT | Gross pay for regular hours |
| `ot_hours` | FLOAT | Overtime hours worked |
| `total_ot_paid` | FLOAT | Total overtime pay |
| `total_other_pay` | FLOAT | Other compensation |
| `loaded_at` | TIMESTAMP | Timestamp when this batch was loaded |

**Row count:** Grows by ~981 rows per pipeline run

---

## Data Flow

```
NYC Open Data API (1000 rows, raw)
         ↓
   api_to_db task
         ↓
staging.nyc_payroll (TRUNCATE + insert ~981 rows after filtering)
         ↓
   validate_staging (year range, negative values, empty check)
         ↓
   db_to_db task (Spark processing)
   ├── dropDuplicates()           removes duplicate rows
   ├── trim(initcap(lower()))     normalizes all string columns
   └── loaded_at = NOW()          tags batch with current timestamp
         ↓
public.nyc_payroll (APPEND ~981 rows per run)
         ↓
   validate_final (batch count, ratio check)
```

---

## Key Differences: Staging vs Public

| Aspect | `staging` | `public` |
|---|---|---|
| Text case | UPPERCASE (raw from API) | Title Case (Spark cleaned) |
| Duplicates | May contain duplicates | Deduplicated |
| Row count | ~981 per run | Grows by ~981 per run |
| Refresh pattern | Full refresh (TRUNCATE) | Append (historical) |
| Timestamp | None | `loaded_at TIMESTAMP` |
| Use case | Debugging, reprocessing | Reporting, dashboards, Grafana |

---

## Validation Rules

| Rule | Applied On | Condition |
|---|---|---|
| Non-empty dataset | `staging` | API returned > 0 rows |
| Fiscal year range | `staging` | `fiscal_year` between 2000 and 2100 |
| No negative pay/hours | `staging` | `regular_hours >= 0` and `regular_gross_paid >= 0` |
| Table existence | `public` | Table must exist before validation |
| Non-empty latest batch | `public` | Latest `loaded_at` batch has > 0 rows |
| Deduplication ratio | `public` | Latest batch >= 30% of staging count |

---

## Useful Queries

```sql
-- Total rows in DB
SELECT COUNT(*) FROM public.nyc_payroll;

-- Rows per batch (pipeline run history)
SELECT loaded_at, COUNT(*)
FROM public.nyc_payroll
GROUP BY loaded_at
ORDER BY loaded_at DESC;

-- Full DB summary
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT loaded_at) AS total_batches,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS latest_load
FROM public.nyc_payroll;

-- Top 10 agencies by headcount (latest batch)
SELECT agency_name, COUNT(*) AS employees
FROM public.nyc_payroll
WHERE loaded_at = (SELECT MAX(loaded_at) FROM public.nyc_payroll)
GROUP BY agency_name
ORDER BY employees DESC
LIMIT 10;

-- Average salary by borough (latest batch)
SELECT work_location_borough, ROUND(AVG(base_salary)::numeric, 2) AS avg_salary
FROM public.nyc_payroll
WHERE loaded_at = (SELECT MAX(loaded_at) FROM public.nyc_payroll)
GROUP BY work_location_borough
ORDER BY avg_salary DESC;
```

---

## Data Source

| Field | Value |
|---|---|
| Provider | NYC Office of Payroll Administration |
| API Endpoint | `https://data.cityofnewyork.us/resource/k397-673e.json` |
| Update Frequency | Annually (fiscal year basis) |
| Full Dataset Size | 5+ million records |
| Pipeline Fetch Limit | 1000 rows per run (Socrata API default) |
| Rows After Filtering | ~981 (negative values removed) |
