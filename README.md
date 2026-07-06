# oracle-de-pipeline

A batch data pipeline from Oracle Autonomous Database (OCI) through a local DuckDB warehouse.
Built as a hands-on learning project covering core Data Engineering patterns:
ELT, star schema, incremental loads, type-safe extraction, and OOP abstractions.

---

## Architecture

```
Oracle Autonomous Database (OCI)
  schema: SH (Sales History)
  user:   DE_LAB (least-privilege)
       │
       │  python-oracledb  (thin mode, wallet/mTLS)
       ▼
  pipeline/extract.py  ←  OracleExtractor(SourceExtractor Protocol)
   ├── dimension tables ──► channels, products, customers, times
   │   SELECT * FROM sh.<table>           (full load each run)
   │
   └── fact table ─────────► sales
       first run:  SELECT * FROM sh.sales           (full load)
       next runs:  WHERE time_id > <watermark>      (incremental)
       batched:    50 000 rows per chunk
       │
       ▼
  data/raw/
   ├── channels_YYYYMMDD.parquet
   ├── products_YYYYMMDD.parquet
   ├── customers_YYYYMMDD.parquet
   ├── times_YYYYMMDD.parquet
   └── sales_YYYYMMDD.parquet
       data/watermark.json  ← last extracted TIME_ID
       │
       │  duckdb
       ▼
  pipeline/load.py
   ├── staging schema  ──► staging.channels / products / customers / times / sales
   │   CREATE OR REPLACE TABLE … AS SELECT * FROM read_parquet(…)
   │
   └── main (core) schema ─► star schema
       ├── main.dim_channels
       ├── main.dim_products
       ├── main.dim_customers
       ├── main.dim_times
       └── main.fct_sales   (incremental INSERT on re-runs)
       │
       ▼
  data/warehouse.duckdb
```

---

## Where this fits in the DE roadmap

| Layer | Phases 1–2 (this project) | Planned |
|---|---|---|
| Storage & Databases | Oracle ADB (source) · Parquet · DuckDB | Snowflake |
| Data Ingestion | Batch extract — watermark incremental | Phase 5: streaming via Kafka / Oracle AQ |
| Data Pipelines | Python + SQL · OOP extractor abstraction | Phase 3: dbt · Phase 4: Airflow |
| Data Warehouse | DuckDB star schema (staging → core) | Phase 3: dbt models on top |
| Version Control | Git + GitHub | Docker, GitHub Actions |

---

## Tech stack

| Tool | Purpose |
|---|---|
| `python-oracledb` (thin) | Oracle DB driver, wallet/mTLS auth, no Oracle Client needed |
| `polars` | DataFrame library — fast, columnar, Rust-backed |
| `pyarrow` | Parquet write backend used by polars |
| `duckdb` | Local analytical warehouse — reads Parquet natively |
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
   .venv\Scripts\activate                                      # Windows
   pip install python-oracledb polars pyarrow python-dotenv duckdb
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

**Step 1 — Extract from Oracle to Parquet:**

```bash
python -m pipeline.extract
```

First run: full load of all dimension tables + full `sh.sales`.
Saves `data/watermark.json` with the max `TIME_ID` from `sales`.

Subsequent runs: dimensions reload in full; `sales` fetches only rows newer than the watermark.

**Step 2 — Load into DuckDB warehouse:**

```bash
python -m pipeline.load
```

Loads Parquet files into `data/warehouse.duckdb`:
- `staging.*` — raw 1:1 copy of Parquet data
- `main.dim_*` / `main.fct_*` — star schema; `fct_sales` uses incremental INSERT on re-runs

**Step 3 — Run analytical queries:**

```bash
python -m pipeline.query
```

Demonstrates revenue aggregations by channel, product, and year, plus window functions.

---

## Key DE concepts demonstrated

**ELT pattern** — data lands in the warehouse first (Load), transformations happen inside
the warehouse (dbt in Phase 3). Staging layer = raw copy; core layer = curated star schema.

**Star schema** — `fct_sales` as the central fact table, four `dim_*` tables around it.
Every analytical query follows a predictable JOIN shape: fact + one or more dimensions.

**Idempotency** — date stamp in Parquet filenames (`sales_20260706.parquet`).
Re-running on the same day overwrites, not appends. `CREATE OR REPLACE TABLE` in DuckDB
ensures the warehouse rebuilds cleanly from scratch on every full run.

**Incremental load (watermark pattern)** — after a full extract, `max(TIME_ID)` is
written to `data/watermark.json`. The next run queries only `WHERE time_id > <watermark>`.
The same pattern is applied inside DuckDB: `fct_sales` only inserts rows newer than its
current `MAX(time_id)`.

**Type precision** — `oracledb.defaults.fetch_decimals = True` forces `AMOUNT_SOLD`
to arrive as Python `Decimal`, not `float`. Preserved through the full round-trip:
Oracle → polars → Parquet → DuckDB (`DECIMAL(38,2)`).

**Batched reads** — `sh.sales` is fetched in chunks of 50 000 rows via
`polars.read_database(..., iter_batches=True, batch_size=50_000)`.
Controls both memory usage and Oracle network round-trips.

**OOP extractor abstraction** — `OracleExtractor` implements the `SourceExtractor` Protocol.
Swapping the source database requires only a new class implementing the same two-method
interface (`extract_dimensions`, `extract_facts`), with no changes to the load or query layer.

**Least privilege** — extraction runs as `DE_LAB`, a dedicated user with read-only
access to `SH`. Not as `ADMIN`.

**Secrets outside code** — credentials and wallet path live in `.env`, excluded
from version control via `.gitignore`.

---

## Project structure

```
oracle-de-pipeline/
├── pipeline/
│   ├── __init__.py
│   ├── extract.py       entry point: get_connection, __main__ orchestration
│   ├── extractors.py    SourceExtractor Protocol + OracleExtractor class
│   ├── io.py            write_parquet, read/save_watermark — shared I/O utilities
│   ├── load.py          staging + core (star schema) load into DuckDB
│   └── query.py         analytical query examples (aggregations, window functions)
├── scripts/
│   ├── check_connection.py   verify Oracle connectivity
│   └── explore_duckdb.py     inspect Parquet files directly via DuckDB
├── data/
│   ├── raw/             Parquet output (gitignored)
│   ├── warehouse.duckdb DuckDB warehouse file (gitignored)
│   └── watermark.json   incremental load bookmark (gitignored)
├── .env                 secrets — never committed
└── .gitignore
```

---

## Roadmap

| Phase | Goal | Key tools | Status |
|---|---|---|---|
| 1 | Extract Oracle → Parquet | python-oracledb, polars | done |
| 2 | Load into a local warehouse | DuckDB, star schema | done |
| 3 | Transform with dbt | dbt-core, dbt-duckdb | next |
| 4 | Orchestrate the pipeline | Apache Airflow | planned |
| 5 | Streaming (Oracle AQ → Kafka) | Kafka, Oracle Advanced Queuing | planned |
