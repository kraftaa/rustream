use anyhow::{Context, Result};
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::{Catalog, CatalogBuilder};
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{CatalogConfig, CatalogType};

/// Build a catalog from the config. Returns an Arc<dyn Catalog> so callers
/// don't need to know the concrete type.
///
/// - **filesystem** (default): Uses a MemoryCatalog backed by FileIO pointed
///   at the warehouse path. Metadata JSON and Parquet live side-by-side.
/// - **glue**: Requires the `glue` feature flag.
pub async fn build_catalog(
    warehouse: &str,
    catalog_config: Option<&CatalogConfig>,
) -> Result<Arc<dyn Catalog>> {
    let catalog_type = catalog_config
        .map(|c| &c.catalog_type)
        .unwrap_or(&CatalogType::Filesystem);

    match catalog_type {
        CatalogType::Filesystem => build_filesystem_catalog(warehouse).await,
        CatalogType::Glue => build_glue_catalog(warehouse, catalog_config.unwrap()).await,
    }
}

async fn build_filesystem_catalog(warehouse: &str) -> Result<Arc<dyn Catalog>> {
    let mut props = HashMap::new();
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.to_string());

    let catalog = MemoryCatalogBuilder::default()
        .load("rustream", props)
        .await
        .context("building filesystem catalog")?;

    tracing::info!(warehouse = %warehouse, "created filesystem catalog");
    Ok(Arc::new(catalog))
}

async fn build_glue_catalog(
    _warehouse: &str,
    catalog_config: &CatalogConfig,
) -> Result<Arc<dyn Catalog>> {
    let _db = catalog_config
        .glue_database
        .as_deref()
        .expect("glue_database validated at config load");

    #[cfg(feature = "glue")]
    {
        anyhow::bail!("Glue catalog support is compiled but not yet wired up. Coming soon.");
    }

    #[cfg(not(feature = "glue"))]
    {
        anyhow::bail!(
            "Glue catalog requires the 'glue' feature. \
             Rebuild with: cargo build --features glue"
        );
    }
}
