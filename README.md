# NYC Payroll ETL Pipeline

An end-to-end data engineering pipeline that extracts NYC payroll data from the NYC Open Data API, transforms it using Apache Spark, validates data quality, and loads it into a PostgreSQL database — fully orchestrated with Apache Airflow, containerized with Docker, and deployed on a live Ubuntu server with CI/CD via GitHub Actions.

---

## Architecture

```
NYC Open Data API
       ↓ (daily @ midnight UTC, automatic)
  Apache Airflow (Orchestration)
       ↓
  staging.nyc_payroll (Raw layer — truncated each run)
       ↓
  Apache Spark (Dedupe + text normalization)
       ↓
  public.nyc_payroll (Append layer — historical batches)
       ↓
  Grafana Dashboard (Live visualization)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.5 |
| Processing | Apache Spark (PySpark) 3.5.4 |
| Database | PostgreSQL 16 |
| Visualization | Grafana |
| Containerization | Docker + Docker Compose |
| Reverse Proxy | Nginx (basic auth protected) |
| CI/CD | GitHub Actions |
| Server | Ubuntu 24.04 VPS |
| Language | Python 3.11 |
| Data Source | NYC Open Data API |

---

## Project Structure

```
nyc-payroll-pipeline/
├── .github/
│   └── workflows/
│       └── deploy.yml            # CI/CD - auto deploy on push to main
├── dags/
│   └── nyc_payroll_pipeline.py   # Airflow DAG definition
├── src/
│   ├── __init__.py
│   ├── extract.py                # API extraction logic
│   └── transform.py              # Spark transformation logic
├── docs/
│   └── schema.md                 # Database schema documentation
├── .env.example                  # Environment variable template
├── docker-compose.yml            # All service definitions
├── Dockerfile                    # Custom Airflow + Java + PySpark image
├── requirements.txt              # Python dependencies
└── README.md
```

---

## Pipeline DAG

```
start → api_to_db → validate_staging → db_to_db → validate_final → end
```

| Task | Description |
|---|---|
| `api_to_db` | Fetches 1000 rows from NYC Open Data API, truncates and inserts into `staging.nyc_payroll` |
| `validate_staging` | Checks row count, fiscal year range (2000–2100), negative hours/pay values |
| `db_to_db` | Reads staging, applies Spark transformations, appends to `public.nyc_payroll` with timestamp |
| `validate_final` | Checks latest batch count and deduplication ratio |

---

## Data Pattern

The pipeline uses a **hybrid pattern**:

- `staging.nyc_payroll` → **Full refresh** (TRUNCATE + insert each run)
- `public.nyc_payroll` → **Append** (each run adds a new batch tagged with `loaded_at` timestamp)

---

## Spark Transformations

- Deduplication via `dropDuplicates()`
- Text normalization: `lower → initcap → trim` on all string columns
- Type-safe column detection (skips numeric and date columns)
- Batch timestamp tagging via `loaded_at`

---

## Data Quality Checks

| Check | Stage | Rule |
|---|---|---|
| Empty dataset | `api_to_db` | Fails if API returns 0 rows |
| Fiscal year range | `validate_staging` | Must be between 2000 and 2100 |
| Negative values | `validate_staging` | `regular_hours` and `regular_gross_paid` must be ≥ 0 |
| Table existence | `validate_final` | Fails if public table doesn't exist |
| Empty latest batch | `validate_final` | Fails if no rows loaded in current run |
| Dedup ratio | `validate_final` | Latest batch must be ≥ 30% of staging count |

---

## Email Alerts

Automatic email alerts sent on any task failure, including exception message and a direct link to task logs in the Airflow UI.

---

## Server Security

| Protection | Details |
|---|---|
| SSH keys only | Password authentication disabled |
| Non-standard SSH port | Default port changed |
| Root login disabled | `PermitRootLogin no` |
| Fail2Ban | Bans IPs after repeated failed SSH attempts |
| UFW Firewall | Only required ports open publicly |
| Nginx basic auth | Airflow and Grafana UI password protected |
| Auto security updates | `unattended-upgrades` enabled |
| Swap space | Added as memory safety net |

---

## Getting Started (Local)

### Prerequisites

- Docker Desktop
- Git

### Setup

**1. Clone the repository:**
```bash
git clone https://github.com/mainulimahi/nyc-payroll-pipeline.git
cd nyc-payroll-pipeline
```

**2. Create your `.env` file:**
```bash
cp .env.example .env
# Edit .env with your credentials
```

**3. Generate secret keys (run twice):**
```bash
docker run --rm python:3.11-slim python -c \
  "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**4. Create required folders:**
```bash
mkdir -p logs plugins data
```

**5. Initialize Airflow:**
```bash
docker compose up airflow-init --build
```

**6. Start all services:**
```bash
docker compose up -d
```

**7. Add Postgres connection in Airflow UI:**

Go to **Admin → Connections → +** and fill in:
- Connection Id: `postgres_conn`
- Connection Type: `Postgres`
- Host: `postgres`
- Database: value from your `.env` `POSTGRES_DB`
- Login/Password: from your `.env`
- Port: `5432`

**8. Trigger the DAG** from the Airflow UI.

---

## CI/CD

Every push to `main` branch automatically:
1. SSHs into the production server
2. Pulls latest code
3. Rebuilds Docker image only if `Dockerfile` or `requirements.txt` changed
4. Restarts Airflow containers with zero downtime
5. Sends email alert if deployment fails

---

## Environment Variables

See `.env.example` for all required variables:

| Variable | Description |
|---|---|
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | PostgreSQL database name |
| `AIRFLOW__CORE__FERNET_KEY` | Encryption key for Airflow secrets |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Flask secret key for Airflow UI |
| `AIRFLOW__SMTP__SMTP_USER` | Gmail address for failure alerts |
| `AIRFLOW__SMTP__SMTP_PASSWORD` | Gmail App Password (no spaces) |
| `ALERT_EMAIL` | Email address to receive failure alerts |

---

## Data Source

| Field | Value |
|---|---|
| Provider | NYC Office of Payroll Administration |
| API Endpoint | `https://data.cityofnewyork.us/resource/k397-673e.json` |
| Update Frequency | Annually (fiscal year basis) |
| Full Dataset Size | 5+ million records |
| Pipeline Fetch Limit | 1000 rows per run (Socrata API default) |

---

## License

MIT
