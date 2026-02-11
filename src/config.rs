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
