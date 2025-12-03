use anyhow::{Context, Result};
use lance::Dataset;
use std::path::PathBuf;

use crate::datasets::path_to_uri;

pub async fn cmd_info(filepath: &PathBuf) -> Result<()> {
    println!("=== Lance File Info ===");
    println!("Path: {}", filepath.display());

    // Open the Lance dataset
    let uri = path_to_uri(filepath);
    let dataset = Dataset::open(&uri)
        .await
        .context("Failed to open Lance dataset")?;

    let schema = dataset.schema();
    let count = dataset.count_rows(None).await;
    let version = dataset.version();

    println!("Version: {}", version.version);
    println!("Rows: {:?}", count);

    println!("\nSchema:");
    for idx in schema.field_ids() {
        let f = schema.field_by_id(idx);
        println!(" - {} : {:?}", idx, f);
    }

    Ok(())
}
