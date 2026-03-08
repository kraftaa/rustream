use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub output: Option<OutputConfig>,
    #[serde(default)]
    pub tables: Option<Vec<TableConfig>>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub state_dir: Option<String>,
    #[serde(default)]
    pub format: OutputFormat,
    #[serde(default)]
    pub catalog: Option<CatalogConfig>,
    #[serde(default)]
    pub warehouse: Option<String>,
    #[serde(default)]
    pub ingest: Option<IngestConfig>,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    #[default]
    Parquet,
    Iceberg,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    #[serde(default = "default_catalog_type", rename = "type")]
    pub catalog_type: CatalogType,
    #[serde(default)]
    pub glue_database: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum CatalogType {
    #[default]
    Filesystem,
    Glue,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PostgresConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum OutputConfig {
    #[serde(rename = "local")]
    Local { path: String },
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        prefix: String,
        #[serde(default)]
        region: Option<String>,
    },
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableConfig {
    pub name: String,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default)]
    pub incremental_column: Option<String>,
    #[serde(default)]
    pub incremental_tiebreaker_column: Option<String>,
    #[serde(default)]
    pub incremental_column_is_unique: bool,
    #[serde(default)]
    pub partition_by: Option<PartitionBy>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PartitionBy {
    Date,
    Month,
    Year,
}

impl TableConfig {
    pub fn full_name(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }

    pub fn schema_or_public(&self) -> &str {
        self.schema.as_deref().unwrap_or("public")
    }
}

// --- Ingest config ---

#[derive(Debug, Deserialize, Clone)]
pub struct IngestConfig {
    pub input: InputConfig,
    #[serde(default = "default_ingest_file_format")]
    pub file_format: FileFormat,
    #[serde(default)]
    pub write_mode: WriteMode,
    #[serde(default = "default_ingest_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub tables: Vec<IngestTableConfig>,
    #[serde(default = "default_schema")]
    pub target_schema: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum InputConfig {
    #[serde(rename = "local")]
    Local {
        path: String,
        #[serde(default = "default_pattern")]
        pattern: String,
    },
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        region: Option<String>,
        #[serde(default = "default_pattern")]
        pattern: String,
    },
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    #[default]
    Parquet,
    Csv,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    Insert,
    Upsert,
    TruncateInsert,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IngestTableConfig {
    pub file_pattern: String,
    pub target_table: String,
    #[serde(default)]
    pub key_columns: Vec<String>,
    #[serde(default)]
    pub create_if_missing: bool,
}

fn default_pattern() -> String {
    "**/*.parquet".to_string()
}

fn default_ingest_file_format() -> FileFormat {
    FileFormat::Parquet
}

fn default_ingest_batch_size() -> usize {
    5000
}

impl PostgresConfig {
    pub fn connection_string(&self) -> String {
        let mut s = format!(
            "host={} port={} dbname={} user={}",
            self.host, self.port, self.database, self.user
        );
        if let Some(ref pw) = self.password {
            s.push_str(&format!(" password={}", pw));
        }
        s
    }
}

fn default_port() -> u16 {
    5432
}

fn default_schema() -> String {
    "public".to_string()
}

fn default_batch_size() -> usize {
    10_000
}

fn default_catalog_type() -> CatalogType {
    CatalogType::Filesystem
}

pub fn load(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(Path::new(path))
        .with_context(|| format!("reading config from {path}"))?;
    let config: Config =
        serde_yaml::from_str(&content).with_context(|| format!("parsing config from {path}"))?;

    // Validate: iceberg format requires a warehouse path
    if config.format == OutputFormat::Iceberg && config.warehouse.is_none() {
        anyhow::bail!("'warehouse' is required when format is 'iceberg'");
    }

    // Validate: glue catalog requires glue_database
    if let Some(ref cat) = config.catalog {
        if matches!(cat.catalog_type, CatalogType::Glue) && cat.glue_database.is_none() {
            anyhow::bail!("'glue_database' is required when catalog type is 'glue'");
        }
    }

    // Validate: upsert mode requires key_columns on each table mapping
    if let Some(ref ingest) = config.ingest {
        if ingest.write_mode == WriteMode::Upsert {
            for table in &ingest.tables {
                if table.key_columns.is_empty() {
                    anyhow::bail!(
                        "ingest table '{}' requires 'key_columns' when write_mode is 'upsert'",
                        table.target_table
                    );
                }
            }
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let yaml = r#"
postgres:
  host: localhost
  database: mydb
  user: me
output:
  type: local
  path: ./out
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.postgres.host, "localhost");
        assert_eq!(config.postgres.port, 5432);
        assert_eq!(config.postgres.database, "mydb");
        assert_eq!(config.postgres.password, None);
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.schema, "public");
        assert!(config.tables.is_none());
        assert!(config.exclude.is_empty());
    }

    #[test]
    fn parse_full_config() {
        let yaml = r#"
postgres:
  host: db.example.com
  port: 5433
  database: prod
  user: admin
  password: secret123
output:
  type: s3
  bucket: my-bucket
  prefix: raw/pg
  region: us-west-2
batch_size: 5000
schema: analytics
state_dir: /tmp/state
tables:
  - name: users
    incremental_column: updated_at
    incremental_tiebreaker_column: id
    incremental_column_is_unique: false
    columns:
      - id
      - email
  - name: orders
    partition_by: date
exclude:
  - migrations
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.postgres.port, 5433);
        assert_eq!(config.postgres.password.as_deref(), Some("secret123"));
        assert_eq!(config.batch_size, 5000);
        assert_eq!(config.schema, "analytics");
        assert_eq!(config.state_dir.as_deref(), Some("/tmp/state"));

        let tables = config.tables.unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "users");
        assert_eq!(tables[0].incremental_column.as_deref(), Some("updated_at"));
        assert_eq!(
            tables[0].incremental_tiebreaker_column.as_deref(),
            Some("id")
        );
        assert!(!tables[0].incremental_column_is_unique);
        assert_eq!(tables[0].columns.as_ref().unwrap().len(), 2);
        assert!(matches!(tables[1].partition_by, Some(PartitionBy::Date)));

        assert_eq!(config.exclude, vec!["migrations"]);

        match config.output.unwrap() {
            OutputConfig::S3 {
                bucket,
                prefix,
                region,
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(prefix, "raw/pg");
                assert_eq!(region.as_deref(), Some("us-west-2"));
            }
            _ => panic!("expected S3 output"),
        }
    }

    #[test]
    fn connection_string_without_password() {
        let pg = PostgresConfig {
            host: "localhost".into(),
            port: 5432,
            database: "testdb".into(),
            user: "maria".into(),
            password: None,
        };
        assert_eq!(
            pg.connection_string(),
            "host=localhost port=5432 dbname=testdb user=maria"
        );
    }

    #[test]
    fn connection_string_with_password() {
        let pg = PostgresConfig {
            host: "db.prod".into(),
            port: 5433,
            database: "prod".into(),
            user: "admin".into(),
            password: Some("s3cret".into()),
        };
        assert_eq!(
            pg.connection_string(),
            "host=db.prod port=5433 dbname=prod user=admin password=s3cret"
        );
    }

    #[test]
    fn table_full_name_with_schema() {
        let t = TableConfig {
            name: "users".into(),
            schema: Some("analytics".into()),
            columns: None,
            incremental_column: None,
            incremental_tiebreaker_column: None,
            incremental_column_is_unique: false,
            partition_by: None,
        };
        assert_eq!(t.full_name(), "analytics.users");
        assert_eq!(t.schema_or_public(), "analytics");
    }

    #[test]
    fn table_full_name_without_schema() {
        let t = TableConfig {
            name: "orders".into(),
            schema: None,
            columns: None,
            incremental_column: None,
            incremental_tiebreaker_column: None,
            incremental_column_is_unique: false,
            partition_by: None,
        };
        assert_eq!(t.full_name(), "orders");
        assert_eq!(t.schema_or_public(), "public");
    }

    #[test]
    fn partition_by_variants() {
        let yaml_date = "\"date\"";
        let yaml_month = "\"month\"";
        let yaml_year = "\"year\"";

        let d: PartitionBy = serde_yaml::from_str(yaml_date).unwrap();
        assert!(matches!(d, PartitionBy::Date));

        let m: PartitionBy = serde_yaml::from_str(yaml_month).unwrap();
        assert!(matches!(m, PartitionBy::Month));

        let y: PartitionBy = serde_yaml::from_str(yaml_year).unwrap();
        assert!(matches!(y, PartitionBy::Year));
    }

    #[test]
    fn load_nonexistent_file() {
        let result = load("/tmp/nonexistent_rustream_config_xyz.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn parse_ingest_config_local() {
        let yaml = r#"
postgres:
  host: localhost
  database: testdb
  user: postgres
output:
  type: local
  path: ./output
ingest:
  input:
    type: local
    path: ./parquet_files
    pattern: "**/*.parquet"
  file_format: parquet
  write_mode: upsert
  batch_size: 3000
  tables:
    - file_pattern: "users/*.parquet"
      target_table: users
      key_columns: [id]
      create_if_missing: true
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let ingest = config.ingest.unwrap();
        assert!(matches!(ingest.input, InputConfig::Local { .. }));
        assert_eq!(ingest.file_format, FileFormat::Parquet);
        assert_eq!(ingest.write_mode, WriteMode::Upsert);
        assert_eq!(ingest.batch_size, 3000);
        assert_eq!(ingest.tables.len(), 1);
        assert_eq!(ingest.tables[0].target_table, "users");
        assert_eq!(ingest.tables[0].key_columns, vec!["id"]);
        assert!(ingest.tables[0].create_if_missing);
    }

    #[test]
    fn parse_ingest_config_s3() {
        let yaml = r#"
postgres:
  host: localhost
  database: testdb
  user: postgres
ingest:
  input:
    type: s3
    bucket: my-bucket
    prefix: raw/data
    region: us-east-1
  file_format: csv
  write_mode: truncate_insert
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let ingest = config.ingest.unwrap();
        match &ingest.input {
            InputConfig::S3 {
                bucket,
                prefix,
                region,
                ..
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(prefix.as_deref(), Some("raw/data"));
                assert_eq!(region.as_deref(), Some("us-east-1"));
            }
            _ => panic!("expected S3 input config"),
        }
        assert_eq!(ingest.file_format, FileFormat::Csv);
        assert_eq!(ingest.write_mode, WriteMode::TruncateInsert);
    }

    #[test]
    fn ingest_config_defaults() {
        let yaml = r#"
postgres:
  host: localhost
  database: testdb
  user: postgres
ingest:
  input:
    type: local
    path: ./data
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let ingest = config.ingest.unwrap();
        assert_eq!(ingest.file_format, FileFormat::Parquet);
        assert_eq!(ingest.write_mode, WriteMode::Insert);
        assert_eq!(ingest.batch_size, 5000);
        assert_eq!(ingest.target_schema, "public");
        assert!(ingest.tables.is_empty());
    }

    #[test]
    fn config_without_ingest() {
        let yaml = r#"
postgres:
  host: localhost
  database: testdb
  user: postgres
output:
  type: local
  path: ./output
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(config.ingest.is_none());
    }

    #[test]
    fn config_without_output() {
        let yaml = r#"
postgres:
  host: localhost
  database: testdb
  user: postgres
ingest:
  input:
    type: local
    path: ./data
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(config.output.is_none());
        assert!(config.ingest.is_some());
    }
}
