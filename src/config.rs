use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub postgres: PostgresConfig,
    pub output: OutputConfig,
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

pub fn load(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(Path::new(path))
        .with_context(|| format!("reading config from {path}"))?;
    let config: Config =
        serde_yaml::from_str(&content).with_context(|| format!("parsing config from {path}"))?;
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
        assert_eq!(tables[0].columns.as_ref().unwrap().len(), 2);
        assert!(matches!(tables[1].partition_by, Some(PartitionBy::Date)));

        assert_eq!(config.exclude, vec!["migrations"]);

        match config.output {
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
}
