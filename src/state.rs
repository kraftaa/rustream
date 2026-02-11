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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn temp_state_dir() -> String {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("rustream_test_{}_{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn open_creates_directory_and_db() {
        let dir = temp_state_dir();
        let _store = StateStore::open(&dir).unwrap();
        assert!(Path::new(&dir).join("rustream_state.db").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_watermark_returns_none_initially() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();
        assert_eq!(store.get_watermark("users").unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn set_and_get_watermark() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store.set_watermark("users", "2026-01-15 10:30:00").unwrap();
        assert_eq!(
            store.get_watermark("users").unwrap().as_deref(),
            Some("2026-01-15 10:30:00")
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_watermark_overwrites() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store
            .set_watermark("orders", "2026-01-01 00:00:00")
            .unwrap();
        store
            .set_watermark("orders", "2026-02-01 00:00:00")
            .unwrap();
        assert_eq!(
            store.get_watermark("orders").unwrap().as_deref(),
            Some("2026-02-01 00:00:00")
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn multiple_tables_independent() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store.set_watermark("users", "aaa").unwrap();
        store.set_watermark("orders", "bbb").unwrap();

        assert_eq!(
            store.get_watermark("users").unwrap().as_deref(),
            Some("aaa")
        );
        assert_eq!(
            store.get_watermark("orders").unwrap().as_deref(),
            Some("bbb")
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn state_persists_across_opens() {
        let dir = temp_state_dir();

        {
            let store = StateStore::open(&dir).unwrap();
            store.set_watermark("users", "persisted").unwrap();
        }

        {
            let store = StateStore::open(&dir).unwrap();
            assert_eq!(
                store.get_watermark("users").unwrap().as_deref(),
                Some("persisted")
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
