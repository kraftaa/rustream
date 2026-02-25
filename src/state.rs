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
                cursor_value TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
        )
        .context("creating watermarks table")?;

        // Backward-compatible migration for existing state DBs.
        let has_cursor_col = conn
            .prepare("PRAGMA table_info(watermarks)")
            .and_then(|mut stmt| {
                let mut rows = stmt.query([])?;
                let mut found = false;
                while let Some(row) = rows.next()? {
                    let col_name: String = row.get(1)?;
                    if col_name == "cursor_value" {
                        found = true;
                        break;
                    }
                }
                Ok(found)
            })
            .context("checking watermarks schema")?;

        if !has_cursor_col {
            conn.execute("ALTER TABLE watermarks ADD COLUMN cursor_value TEXT", [])
                .context("adding cursor_value column to watermarks")?;
        }

        Ok(Self { conn })
    }

    /// Get high watermark and cursor for a table.
    pub fn get_progress(&self, table_name: &str) -> Result<Option<(String, Option<String>)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT watermark_value, cursor_value FROM watermarks WHERE table_name = ?1")
            .context("preparing progress select")?;

        let result = stmt
            .query_row([table_name], |row| {
                let wm: String = row.get(0)?;
                let cursor: Option<String> = row.get(1)?;
                Ok((wm, cursor))
            })
            .ok();

        Ok(result)
    }

    /// Set high watermark and cursor for a table.
    pub fn set_progress(
        &self,
        table_name: &str,
        watermark_value: &str,
        cursor_value: Option<&str>,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO watermarks (table_name, watermark_value, cursor_value, updated_at)
                 VALUES (?1, ?2, ?3, datetime('now'))
                 ON CONFLICT(table_name) DO UPDATE SET
                    watermark_value = excluded.watermark_value,
                    cursor_value = excluded.cursor_value,
                    updated_at = excluded.updated_at",
                (table_name, watermark_value, cursor_value),
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
    fn get_progress_returns_none_initially() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();
        assert_eq!(store.get_progress("users").unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn set_and_get_watermark_without_cursor() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store
            .set_progress("users", "2026-01-15 10:30:00", None)
            .unwrap();
        assert_eq!(
            store.get_progress("users").unwrap(),
            Some(("2026-01-15 10:30:00".to_string(), None))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_watermark_overwrites() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store
            .set_progress("orders", "2026-01-01 00:00:00", None)
            .unwrap();
        store
            .set_progress("orders", "2026-02-01 00:00:00", None)
            .unwrap();
        assert_eq!(
            store.get_progress("orders").unwrap(),
            Some(("2026-02-01 00:00:00".to_string(), None))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn multiple_tables_independent() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store.set_progress("users", "aaa", None).unwrap();
        store.set_progress("orders", "bbb", None).unwrap();

        assert_eq!(
            store.get_progress("users").unwrap(),
            Some(("aaa".to_string(), None))
        );
        assert_eq!(
            store.get_progress("orders").unwrap(),
            Some(("bbb".to_string(), None))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn state_persists_across_opens() {
        let dir = temp_state_dir();

        {
            let store = StateStore::open(&dir).unwrap();
            store.set_progress("users", "persisted", None).unwrap();
        }

        {
            let store = StateStore::open(&dir).unwrap();
            assert_eq!(
                store.get_progress("users").unwrap(),
                Some(("persisted".to_string(), None))
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn set_and_get_progress() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store
            .set_progress("users", "2026-01-15 10:30:00", Some("42"))
            .unwrap();

        assert_eq!(
            store.get_progress("users").unwrap(),
            Some(("2026-01-15 10:30:00".to_string(), Some("42".to_string())))
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
