# oracle-de-pipeline

A batch data pipeline extracting from Oracle Autonomous Database (OCI) to Parquet.
Built as a hands-on learning project covering core Data Engineering concepts:
idempotent writes, type-safe extraction, batched reads, and incremental loads.

---

## Architecture

```
Oracle Autonomous Database (OCI)
  schema: SH (Sales History)
  user:   DE_LAB (least-privilege)
       │
       │  python-oracledb  (thin mode, wallet/mTLS)
       ▼
  extract.py
   ├── dimension tables ──► channels, products, customers, times
   │   SELECT * FROM sh.<table>          (full load each run)
   │
   └── fact table ─────────► sales (~919k rows)
       first run:  SELECT * FROM sh.sales          (full load)
       next runs:  WHERE time_id > <watermark>     (incremental)
       batched:    50 000 rows per chunk (iter_batches)
       │
       ▼
  data/raw/
   ├── channels_YYYYMMDD.parquet
   ├── products_YYYYMMDD.parquet
   ├── customers_YYYYMMDD.parquet
   ├── times_YYYYMMDD.parquet
   └── sales_YYYYMMDD.parquet
       data/watermark.json  ← last extracted TIME_ID
```

---

## Where this fits in the DE roadmap

This project covers the first two layers of a production DE stack:

| Layer | This project | Planned |
|---|---|---|
| Storage & Databases | Oracle ADB (source) + Parquet (local file storage) | — |
| Data Ingestion | Batch extract — watermark-based incremental | Etap 5: streaming via Kafka / Oracle AQ |
| Data Pipelines | Python + SQL | Etap 3: dbt (transformation), Etap 4: Airflow (orchestration) |
| Data Warehouse | — | Etap 2: DuckDB → Snowflake |
| Version Control | Git + GitHub | Docker, GitHub Actions |

---

## Tech stack

| Tool | Purpose |
|---|---|
| `python-oracledb` (thin) | Oracle DB driver, wallet/mTLS auth, no Oracle Client needed |
| `polars` | DataFrame library — fast, columnar, Rust-backed |
| `pyarrow` | Parquet write backend used by polars |
| `python-dotenv` | Load secrets from `.env` at runtime |

---

## Prerequisites

- Python 3.11+
- Oracle Autonomous Database (OCI) with the sample `SH` schema
- Wallet files (`tnsnames.ora` + `ewallet.pem`) downloaded from OCI console
- A low-privilege DB user with `SELECT ANY TABLE` on the `SH` schema

---

## Setup

1. Clone the repo and create a virtual environment:

   ```bash
   python -m venv .venv
   .venv\Scripts\activate        # Windows
   pip install python-oracledb polars pyarrow python-dotenv
   ```

2. Create `.env` in the project root (never commit this file):

   ```
   ORACLE_USER=your_user
   ORACLE_PASSWORD=your_password
   ORACLE_DSN=your_tns_alias
   WALLET_DIR=C:\path\to\wallet
   WALLET_PASSWORD=your_wallet_password
   ```

3. Place wallet files (`tnsnames.ora`, `ewallet.pem`) in the directory set as `WALLET_DIR`.

---

## Usage

```bash
python extract.py
```

**First run:** full load of all dimension tables + full `sh.sales` (~919k rows).
Saves `data/watermark.json` with the max `TIME_ID` from `sales`.

**Subsequent runs:** dimension tables reload in full; `sales` fetches only rows
with `time_id` newer than the saved watermark (incremental).

Output lands in `data/raw/` as date-stamped Parquet files.
Running the script again on the same day overwrites that day's file (idempotent).

---

## Key DE concepts demonstrated

**Idempotency** — date stamp in the filename (`sales_20260702.parquet`).
Re-running on the same day overwrites, not appends. No duplicates.

**Type precision** — `oracledb.defaults.fetch_decimals = True` forces `AMOUNT_SOLD`
to arrive as Python `Decimal`, not `float`. Financial columns must never be floats
(`0.1 + 0.2 ≠ 0.3` in binary floating point).

**Batched reads** — `sh.sales` (~919k rows) is fetched in chunks of 50 000 via
`polars.read_database(..., iter_batches=True, batch_size=50_000)`.
Controls both memory usage and network round-trips (`arraysize`).

**Incremental load (watermark pattern)** — after a full load, `max(TIME_ID)` is
written to `data/watermark.json`. The next run queries only `WHERE time_id > <watermark>`,
avoiding a full re-scan of the fact table.

**Least privilege** — extraction runs as `DE_LAB`, a dedicated user with read-only
access to `SH`. Not as `ADMIN`.

**Secrets outside code** — credentials and wallet path live in `.env`, excluded
from version control via `.gitignore`.

---

## Project structure

```
oracle-de-pipeline/
├── extract.py          main pipeline script
├── check_connection.py diagnostic: verifies DB connectivity
├── data/
│   └── raw/            Parquet output (gitignored)
│       watermark.json  incremental load bookmark (gitignored)
├── .env                secrets — never committed
└── .gitignore
```

---

## Roadmap

| Etap | Goal | Key tools |
|---|---|---|
| 1 (this) | Extract Oracle → Parquet | python-oracledb, polars |
| 2 | Load into a warehouse | DuckDB (local) → Snowflake |
| 3 | Transform with dbt | dbt-core, dbt-snowflake |
| 4 | Orchestrate the pipeline | Apache Airflow |
| 5 | Streaming (Oracle AQ → Kafka) | Kafka, Oracle Advanced Queuing |
