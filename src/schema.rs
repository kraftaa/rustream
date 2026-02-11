use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use tokio_postgres::Client;

use crate::config::TableConfig;
use crate::types::pg_type_to_arrow;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColumnInfo {
    pub name: String,
    pub pg_type: String,
    pub arrow_type: DataType,
    pub is_nullable: bool,
}

/// Introspect column types for a table from information_schema.
pub async fn introspect_table(client: &Client, table: &TableConfig) -> Result<Vec<ColumnInfo>> {
    let schema_name = table.schema_or_public();

    let query = r#"
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
    "#;

    let rows = client
        .query(query, &[&schema_name, &table.name])
        .await
        .with_context(|| format!("introspecting table {}", table.full_name()))?;

    let mut columns = Vec::new();
    for row in rows {
        let name: String = row.get(0);
        let pg_type: String = row.get(1);
        let nullable_str: String = row.get(2);

        // If specific columns were requested, filter
        if let Some(ref cols) = table.columns {
            if !cols.iter().any(|c| c == &name) {
                continue;
            }
        }

        let arrow_type = pg_type_to_arrow(&pg_type);
        let is_nullable = nullable_str == "YES";

        columns.push(ColumnInfo {
            name,
            pg_type,
            arrow_type,
            is_nullable,
        });
    }

    Ok(columns)
}

/// Discover all tables in a schema.
pub async fn discover_tables(client: &Client, schema_name: &str) -> Result<Vec<String>> {
    let query = r#"
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = $1
        ORDER BY tablename
    "#;

    let rows = client
        .query(query, &[&schema_name])
        .await
        .context("discovering tables")?;

    let tables = rows.iter().map(|r| r.get(0)).collect();
    Ok(tables)
}

/// Build an Arrow Schema from column info.
pub fn build_arrow_schema(columns: &[ColumnInfo]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.arrow_type.clone(), c.is_nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_schema_from_columns() {
        let columns = vec![
            ColumnInfo {
                name: "id".into(),
                pg_type: "integer".into(),
                arrow_type: DataType::Int32,
                is_nullable: false,
            },
            ColumnInfo {
                name: "email".into(),
                pg_type: "text".into(),
                arrow_type: DataType::Utf8,
                is_nullable: true,
            },
            ColumnInfo {
                name: "active".into(),
                pg_type: "boolean".into(),
                arrow_type: DataType::Boolean,
                is_nullable: true,
            },
        ];

        let schema = build_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 3);

        let id_field = schema.field(0);
        assert_eq!(id_field.name(), "id");
        assert_eq!(*id_field.data_type(), DataType::Int32);
        assert!(!id_field.is_nullable());

        let email_field = schema.field(1);
        assert_eq!(email_field.name(), "email");
        assert_eq!(*email_field.data_type(), DataType::Utf8);
        assert!(email_field.is_nullable());
    }

    #[test]
    fn build_schema_empty_columns() {
        let schema = build_arrow_schema(&[]);
        assert_eq!(schema.fields().len(), 0);
    }
}
