use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

pub struct StateStore {
    conn: Connection,
}

impl StateStore {
    /// Open or create the SQLite state database.
    pub fn open(state_dir: &str) -> Result<Self> {
        let dir = Path::new(state_dir);
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating state directory {}", dir.display()))?;

        let db_path = dir.join("rustream_state.db");
        let conn = Connection::open(&db_path)
            .with_context(|| format!("opening state db at {}", db_path.display()))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS watermarks (
                table_name TEXT PRIMARY KEY,
                watermark_value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
        )
        .context("creating watermarks table")?;

        Ok(Self { conn })
    }

    /// Get the high watermark for a table.
    pub fn get_watermark(&self, table_name: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT watermark_value FROM watermarks WHERE table_name = ?1")
            .context("preparing watermark select")?;

        let result = stmt.query_row([table_name], |row| row.get(0)).ok();

        Ok(result)
    }

    /// Set the high watermark for a table.
    pub fn set_watermark(&self, table_name: &str, value: &str) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO watermarks (table_name, watermark_value, updated_at)
                 VALUES (?1, ?2, datetime('now'))
                 ON CONFLICT(table_name) DO UPDATE SET
                    watermark_value = excluded.watermark_value,
                    updated_at = excluded.updated_at",
                [table_name, value],
            )
            .with_context(|| format!("setting watermark for {table_name}"))?;

        Ok(())
    }
}
