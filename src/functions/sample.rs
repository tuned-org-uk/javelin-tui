use anyhow::Result;
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use lance::Dataset;
use log::{debug, info};
use std::path::PathBuf;
use std::sync::Arc;

use crate::datasets::path_to_uri;
use crate::display::display::display_spreadsheet_interactive;
use crate::functions::functions::normalize_for_display;

/// Randomly sample `n_rows` rows from a Lance dataset and show them
/// in the interactive spreadsheet viewer.
pub async fn cmd_sample(filepath: &PathBuf, n_rows: usize) -> Result<()> {
    use rand::rng;
    use rand::seq::SliceRandom;

    let uri = path_to_uri(filepath);
    let dataset = Dataset::open(&uri).await?;

    // Count total rows
    let total_rows = dataset.count_rows(None).await?;

    if total_rows == 0 {
        println!("No data to display");
        return Ok(());
    }

    // Clamp to dataset size
    let n = n_rows.min(total_rows);

    // Generate random indices
    let mut rng = rng();
    let mut indices: Vec<i64> = (0..total_rows as i64).collect();
    indices.shuffle(&mut rng);
    indices.truncate(n);
    indices.sort_unstable();

    // Read all rows up to the max sampled index
    let max_index = *indices.last().unwrap();
    let mut scanner = dataset.scan();
    let full_batch = scanner
        .limit(Some(max_index + 1), None)?
        .try_into_batch()
        .await?;

    if full_batch.num_rows() == 0 {
        println!("No data to display");
        return Ok(());
    }

    // Take only the sampled rows
    let index_array = Arc::new(arrow::array::UInt64Array::from(
        indices.iter().map(|&i| i as u64).collect::<Vec<_>>(),
    )) as ArrayRef;

    let mut sampled_columns = Vec::with_capacity(full_batch.num_columns());
    for col in full_batch.columns().iter() {
        let taken = take(col.as_ref(), &index_array, None)?;
        sampled_columns.push(taken);
    }

    // Create sampled batch with same schema
    let batch = RecordBatch::try_new(full_batch.schema(), sampled_columns)?;

    if batch.num_rows() == 0 {
        println!("No data to display");
        return Ok(());
    }

    let batch = normalize_for_display(&batch)?;
    display_spreadsheet_interactive(&batch)?;
    Ok(())
}
