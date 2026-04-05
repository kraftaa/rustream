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
            );
            CREATE TABLE IF NOT EXISTS ingested_files (
                file_key TEXT PRIMARY KEY,
                target_table TEXT NOT NULL,
                rows_ingested INTEGER NOT NULL,
                ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .context("creating state tables")?;

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

    /// Check if a file has already been ingested.
    pub fn is_file_ingested(&self, file_key: &str) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare("SELECT 1 FROM ingested_files WHERE file_key = ?1")
            .context("preparing ingested_files select")?;

        let exists = stmt.exists([file_key]).context("checking ingested file")?;
        Ok(exists)
    }

    /// Mark a file as ingested.
    pub fn mark_file_ingested(
        &self,
        file_key: &str,
        target_table: &str,
        rows_ingested: u64,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO ingested_files (file_key, target_table, rows_ingested, ingested_at)
                 VALUES (?1, ?2, ?3, datetime('now'))
                 ON CONFLICT(file_key) DO UPDATE SET
                    target_table = excluded.target_table,
                    rows_ingested = excluded.rows_ingested,
                    ingested_at = excluded.ingested_at",
                rusqlite::params![file_key, target_table, rows_ingested as i64],
            )
            .with_context(|| format!("marking file as ingested: {file_key}"))?;

        Ok(())
    }

    /// Clear stored watermark/cursor for a table (used when state is incompatible with schema).
    pub fn clear_progress(&self, table_name: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM watermarks WHERE table_name = ?1", [table_name])
            .with_context(|| format!("clearing progress for {table_name}"))?;
        Ok(())
    }

    /// Clear progress rows by table reference.
    /// For bare names (e.g. `users`), clears both `users` and schema-qualified keys like `analytics.users`.
    pub fn clear_progress_by_table_ref(&self, table_ref: &str) -> Result<usize> {
        let deleted = if table_ref.contains('.') {
            self.conn
                .execute("DELETE FROM watermarks WHERE table_name = ?1", [table_ref])
                .with_context(|| format!("clearing progress for {table_ref}"))?
        } else {
            let suffix = format!("%.{}", table_ref);
            self.conn
                .execute(
                    "DELETE FROM watermarks WHERE table_name = ?1 OR table_name LIKE ?2",
                    rusqlite::params![table_ref, suffix],
                )
                .with_context(|| {
                    format!("clearing progress for {table_ref} and schema-qualified variants")
                })?
        };
        Ok(deleted)
    }

    /// Clear all progress rows.
    pub fn clear_all_progress(&self) -> Result<()> {
        self.conn
            .execute("DELETE FROM watermarks", [])
            .context("clearing all progress")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
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
    fn ingested_files_tracking() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        assert!(!store.is_file_ingested("users/part-0.parquet").unwrap());

        store
            .mark_file_ingested("users/part-0.parquet", "users", 1000)
            .unwrap();
        assert!(store.is_file_ingested("users/part-0.parquet").unwrap());
        assert!(!store.is_file_ingested("users/part-1.parquet").unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ingested_file_update() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store
            .mark_file_ingested("orders/data.parquet", "orders", 500)
            .unwrap();
        store
            .mark_file_ingested("orders/data.parquet", "orders", 750)
            .unwrap();
        assert!(store.is_file_ingested("orders/data.parquet").unwrap());
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
    fn clear_progress_by_table_ref_clears_bare_and_schema_qualified() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store.set_progress("users", "a", None).unwrap();
        store.set_progress("analytics.users", "b", None).unwrap();
        store.set_progress("sales.users", "c", None).unwrap();
        store.set_progress("orders", "d", None).unwrap();

        let deleted = store.clear_progress_by_table_ref("users").unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(store.get_progress("users").unwrap(), None);
        assert_eq!(store.get_progress("analytics.users").unwrap(), None);
        assert_eq!(store.get_progress("sales.users").unwrap(), None);
        assert!(store.get_progress("orders").unwrap().is_some());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn clear_progress_by_table_ref_schema_qualified_exact_only() {
        let dir = temp_state_dir();
        let store = StateStore::open(&dir).unwrap();

        store.set_progress("analytics.users", "b", None).unwrap();
        store.set_progress("sales.users", "c", None).unwrap();

        let deleted = store
            .clear_progress_by_table_ref("analytics.users")
            .unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(store.get_progress("analytics.users").unwrap(), None);
        assert!(store.get_progress("sales.users").unwrap().is_some());

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

    /// Verifies opening state migrates legacy watermark schema and preserves existing values.
    #[test]
    fn open_migrates_legacy_watermarks_table() {
        let dir = temp_state_dir();
        fs::create_dir_all(&dir).unwrap();
        let db_path = Path::new(&dir).join("rustream_state.db");

        // Simulate an old state db that predates cursor_value.
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE watermarks (
                table_name TEXT PRIMARY KEY,
                watermark_value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO watermarks (table_name, watermark_value) VALUES (?1, ?2)",
            ["users", "2026-01-01 00:00:00"],
        )
        .unwrap();
        drop(conn);

        let store = StateStore::open(&dir).unwrap();
        assert_eq!(
            store.get_progress("users").unwrap(),
            Some(("2026-01-01 00:00:00".to_string(), None))
        );

        // Verify migrated schema contains cursor_value.
        let conn = Connection::open(&db_path).unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info(watermarks)").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut has_cursor = false;
        while let Some(row) = rows.next().unwrap() {
            let col_name: String = row.get(1).unwrap();
            if col_name == "cursor_value" {
                has_cursor = true;
                break;
            }
        }
        assert!(has_cursor);

        let _ = fs::remove_dir_all(&dir);
    }
}
