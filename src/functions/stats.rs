use anyhow::Result;
use lance::Dataset;
use std::path::PathBuf;

use crate::datasets::path_to_uri;

pub async fn cmd_stats(filepath: &PathBuf) -> Result<()> {
    println!("=== Dataset Statistics ===");

    let uri = path_to_uri(filepath);
    let dataset = Dataset::open(&uri).await?;
    let schema = dataset.schema();
    let count = dataset.count_rows(None).await?;

    println!("Total rows: {}", count);
    println!("Schema: {}", schema.to_string());

    // Compute basic stats per column (structure‑only here; you can add real stats)
    println!("\nColumn statistics:");
    for idx in schema.field_ids() {
        let f = schema.field_by_id(idx).unwrap();
        println!(" {}:", f.to_string());
        println!(" Type: {:?}", f.data_type());
        println!(" - {} : {:?}", idx, f);
    }

    Ok(())
}
