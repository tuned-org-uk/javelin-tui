use anyhow::Result;
use anyhow::anyhow;

use arrow_array::RecordBatch;
use lance::dataset::Dataset;
use log::{debug, info};

use std::path::PathBuf;

use crate::datasets::path_to_uri;
use crate::display::display::display_spreadsheet_interactive;
use crate::functions::functions::normalize_for_display;

pub async fn cmd_display(filepath: &PathBuf) -> Result<()> {
    info!("cmd_display: opening full dataset at {:?}", filepath);

    let uri = path_to_uri(filepath);
    debug!("cmd_display: Lance URI = {}", uri);

    let dataset = Dataset::open(&uri).await?;
    // Load the entire dataset into a single RecordBatch.
    // For large datasets you may want to stream or limit rows instead.
    let scanner = dataset.scan();
    let batch: RecordBatch = scanner
        .try_into_batch()
        .await
        .map_err(|e| anyhow!("cmd_display: failed to read full batch: {e}"))?;

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    info!(
        "cmd_display: loaded full batch with {} rows × {} cols",
        num_rows, num_cols
    );

    if num_cols == 0 {
        println!("No columns to display");
        return Err(anyhow!("cmd_display: abort, no columns in dataset"));
    }

    if num_rows == 0 {
        println!("Dataset is empty");
        return Ok(());
    }

    let batch = normalize_for_display(&batch)?;
    // Reuse the interactive viewer.
    display_spreadsheet_interactive(&batch)?;
    Ok(())
}
