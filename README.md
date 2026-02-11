# rustream

Fast Postgres to Parquet sync tool. Reads tables from Postgres, writes Parquet files to local disk or S3. Supports incremental sync via `updated_at` watermark tracking.

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

```bash
# Copy and edit the example config
cp config.example.yaml config.yaml

# Preview what will be synced (no files written)
rustream sync --config config.yaml --dry-run

# Run sync
rustream sync --config config.yaml
```

Enable debug logging with `RUST_LOG`:

```bash
RUST_LOG=rustream=debug rustream sync --config config.yaml
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
    columns:          # optional: pick specific columns
      - id
      - email
      - created_at
      - updated_at

  - name: orders
    incremental_column: updated_at

  - name: products    # no incremental_column = full sync every run
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

### Config reference

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
| `tables[].partition_by` | Partition output files: `date`, `month`, or `year` |

## How it works

1. Connects to Postgres and introspects each table's schema via `information_schema`
2. Maps Postgres column types to Arrow types automatically
3. Reads rows in batches, converting to Arrow RecordBatches
4. Writes each batch as a Snappy-compressed Parquet file
5. Tracks the high watermark (max value of `incremental_column`) in local SQLite
6. On next run, only reads rows where `incremental_column > last_watermark`

Tables without `incremental_column` do a full sync every run.

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
