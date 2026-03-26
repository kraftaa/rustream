# rustream

Bidirectional Postgres sync tool. Reads tables from Postgres and writes Parquet/Iceberg files to local disk or S3, or ingests Parquet/CSV files from local disk or S3 back into Postgres. Supports incremental sync via watermark tracking and upsert-based ingestion.

## Installation

### From PyPI

```bash
pipx install rustream
# or
pip install rustream
```

### From source

```bash
git clone https://github.com/kraftaa/rustream.git
cd rustream
cargo build --release
# binary is at target/release/rustream
```

### With maturin (local dev)

```bash
pip install maturin
maturin develop --release
# now `rustream` is on your PATH
```

## Usage

### Sync (Postgres → Parquet/S3)

```bash
# Copy and edit the example config
cp config.example.yaml config.yaml

# Preview what will be synced (no files written)
rustream sync --config config.yaml --dry-run

# Run sync
rustream sync --config config.yaml

# If you need to wipe saved watermarks/cursors (full resync)
rustream sync --config config.yaml --reset-state

# Reset state for one table
rustream reset-state --table users
```

### Ingest (S3/local → Postgres)

```bash
# Preview what would be ingested
rustream ingest --config ingest_config.yaml --dry-run

# Run ingest
rustream ingest --config ingest_config.yaml
```

Enable debug logging with `RUST_LOG`:

```bash
RUST_LOG=rustream=debug rustream sync --config config.yaml
RUST_LOG=rustream=debug rustream ingest --config ingest_config.yaml
```

### Jobs / Worker (experimental)

```
# create control-plane table in Postgres
rustream init-jobs --control-db-url "$CONTROL_DB_URL"

# enqueue a table sync job
rustream add-job --control-db-url "$CONTROL_DB_URL" --table users --config config.yaml --interval-secs 300 --timeout-secs 900 --max-concurrent-jobs 1

# run the worker loop (polls every 5s by default, runs up to 4 jobs at once)
rustream worker --control-db-url "$CONTROL_DB_URL" --poll-seconds 5 --max-concurrent 4

# force a job to run ASAP
rustream force-job --control-db-url "$CONTROL_DB_URL" --job-id 1

# status API (optional, returns JSON)
rustream status-api --control-db-url "$CONTROL_DB_URL" --bind 0.0.0.0:8080

# control DB URL can also come from env
# RUSTREAM_CONTROL_DB_URL=postgres://user:pass@host:5432/db rustream worker
# status endpoints:
#   /jobs (json, optional ?status=pending), /jobs/html (auto-refresh + filter),
#   /jobs/summary, /logs?limit=50, /health, /health/worker
#   UI buttons: force run, retry failed, reset state

# optional: run a data-quality command on each local output table
# (use {path} placeholder for the Parquet directory)
RUSTREAM_DQ_CMD="dq-prof --input {path}" rustream worker --control-db-url "$CONTROL_DB_URL"
```

## Configuration

### Specific tables (recommended)

```yaml
postgres:
  host: localhost
  database: mydb
  user: postgres
  password: secret

output:
  type: local
  path: ./output

tables:
  - name: users
    incremental_column: updated_at
    incremental_tiebreaker_column: id
    columns:          # optional: pick specific columns
      - id
      - email
      - created_at
      - updated_at

  - name: orders
    incremental_column: updated_at
    incremental_tiebreaker_column: id

  - name: products    # no incremental_column = full sync every run

  - name: events      # append-only example (no updated_at)
    incremental_column: id
    incremental_column_is_unique: true
```

### All tables (auto-discover)

Omit `tables` to sync every table in the schema. Use `exclude` to skip some:

```yaml
postgres:
  host: localhost
  database: mydb
  user: postgres

output:
  type: local
  path: ./output

# schema: public    # default
exclude:
  - schema_migrations
  - ar_internal_metadata
```

### S3 output

```yaml
output:
  type: s3
  bucket: my-data-lake
  prefix: raw/postgres
  region: us-east-1
```

AWS credentials come from environment variables, `~/.aws/credentials`, or IAM role.

### Iceberg output

```yaml
output:
  type: s3
  bucket: my-data-lake
  prefix: warehouse
  region: us-east-1

format: iceberg
warehouse: s3://my-data-lake/warehouse
catalog:
  type: filesystem    # or "glue" (requires --features glue)
  # glue_database: my_db  # required when type=glue
```

### Ingest (S3 → Postgres)

```yaml
postgres:
  host: localhost
  database: mydb
  user: postgres
  password: secret

ingest:
  input:
    type: s3
    bucket: my-data-lake
    prefix: raw/postgres/
    region: us-east-1
    pattern: "**/*.parquet"

  file_format: parquet       # "parquet" or "csv"
  write_mode: upsert         # "insert" | "upsert" | "truncate_insert"
  batch_size: 5000
  target_schema: public

  tables:
    - file_pattern: "users/*.parquet"
      target_table: users
      key_columns: [id]
      create_if_missing: true

    - file_pattern: "orders/*.parquet"
      target_table: orders
      key_columns: [id]
```

### Ingest from local files

```yaml
ingest:
  input:
    type: local
    path: ./parquet_files
    pattern: "**/*.parquet"

  file_format: parquet
  write_mode: insert
  batch_size: 5000
```

If no `tables` are listed, the target table name is inferred from the parent directory or filename.

### Config reference (sync)

| Field | Description |
|---|---|
| `postgres.host` | Postgres host |
| `postgres.port` | Postgres port (default: 5432) |
| `postgres.database` | Database name |
| `postgres.user` | Database user |
| `postgres.password` | Database password (optional) |
| `output.type` | `local` or `s3` |
| `output.path` | Local directory for Parquet files (when type=local) |
| `output.bucket` | S3 bucket (when type=s3) |
| `output.prefix` | S3 key prefix (when type=s3) |
| `output.region` | AWS region (when type=s3, optional) |
| `batch_size` | Rows per Parquet file (default: 10000) |
| `state_dir` | Directory for SQLite watermark state (default: `.rustream_state`) |
| `schema` | Schema to discover tables from (default: `public`) |
| `exclude` | List of table names to skip when using auto-discovery |
| `tables[].name` | Table name |
| `tables[].schema` | Schema name (default: `public`) |
| `tables[].columns` | Columns to sync (default: all) |
| `tables[].incremental_column` | Column for watermark-based incremental sync |
| `tables[].incremental_tiebreaker_column` | Stable cursor column for duplicate-safe incremental paging (required when `incremental_column` is set; recommended: primary key) |
| `tables[].incremental_column_is_unique` | Allow watermark-only incremental mode when incremental column is strictly unique/monotonic (e.g. append-only `id`) |
| `tables[].partition_by` | Partition output files: `date`, `month`, or `year` |
| `format` | Output format: `parquet` (default) or `iceberg` |
| `warehouse` | Warehouse path for Iceberg (required when format=iceberg) |
| `catalog.type` | Iceberg catalog: `filesystem` (default) or `glue` |

### Config reference (ingest)

| Field | Description |
|---|---|
| `ingest.input.type` | `local` or `s3` |
| `ingest.input.path` | Local directory (when type=local) |
| `ingest.input.bucket` | S3 bucket (when type=s3) |
| `ingest.input.prefix` | S3 key prefix (when type=s3) |
| `ingest.input.region` | AWS region (when type=s3, optional) |
| `ingest.input.pattern` | Glob pattern for file matching (default: `**/*.parquet`) |
| `ingest.file_format` | `parquet` (default) or `csv` |
| `ingest.write_mode` | `insert` (default), `upsert`, or `truncate_insert` |
| `ingest.batch_size` | Rows per INSERT statement (default: 5000) |
| `ingest.target_schema` | Postgres schema for target tables (default: `public`) |
| `ingest.tables[].file_pattern` | Glob pattern to match files to this table |
| `ingest.tables[].target_table` | Postgres table to write to |
| `ingest.tables[].key_columns` | Primary key columns (required for upsert mode) |
| `ingest.tables[].create_if_missing` | Auto-CREATE TABLE from file schema (default: false) |

## Running Integration Tests

Some DB-backed tests are optional and run only when `RUSTREAM_IT_DB_URL` is set.
Without this env var, those tests no-op/return early.

```bash
export RUSTREAM_IT_DB_URL="host=localhost port=5432 dbname=mydb user=postgres password=secret"
cargo test
```

## How it works

### Sync (Postgres → Parquet)

1. Connects to Postgres and introspects each table's schema via `information_schema`
2. Maps Postgres column types to Arrow types automatically
3. Reads rows in batches, converting to Arrow RecordBatches
4. Writes each batch as a Snappy-compressed Parquet file
5. Tracks the high watermark (max value of `incremental_column`) and optional cursor in local SQLite
6. Checkpoints incremental progress after each successfully written batch
7. On next run, reads rows after the saved `(watermark, cursor)` position

Tables without `incremental_column` do a full sync every run.

### Ingest (Parquet/CSV → Postgres)

1. Discovers files matching the glob pattern from local disk or S3
2. Skips files already ingested (tracked in local SQLite)
3. Reads each file into Arrow RecordBatches (Parquet or CSV with schema inference)
4. Creates the target table if `create_if_missing: true` (DDL from Arrow schema)
5. Writes rows via multi-row parameterized INSERT or INSERT...ON CONFLICT (upsert)
6. Marks each file as ingested in SQLite to avoid reprocessing on next run

## Supported Postgres types

| Postgres | Arrow |
|---|---|
| `boolean` | Boolean |
| `smallint` | Int16 |
| `integer`, `serial` | Int32 |
| `bigint`, `bigserial` | Int64 |
| `real` | Float32 |
| `double precision` | Float64 |
| `numeric` / `decimal` | Utf8 (preserves precision) |
| `text`, `varchar`, `char` | Utf8 |
| `bytea` | Binary |
| `date` | Date32 |
| `timestamp` | Timestamp(Microsecond) |
| `timestamptz` | Timestamp(Microsecond, UTC) |
| `uuid` | Utf8 |
| `json`, `jsonb` | Utf8 |
| arrays | Utf8 (JSON serialized) |

## Publishing

The project uses [maturin](https://github.com/PyO3/maturin) to package the Rust binary as a Python wheel (same approach as ruff, uv, etc). The CI workflow in `.github/workflows/release.yml` builds wheels for Linux, macOS, and Windows, then publishes to PyPI on tagged releases.

To publish manually:

```bash
# Build wheels for current platform
maturin build --release

# Upload to PyPI (needs PYPI_API_TOKEN)
maturin publish
```

## License

MIT
