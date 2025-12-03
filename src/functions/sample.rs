use anyhow::Result;
use arrow::compute::take;
use arrow_array::{ArrayRef, RecordBatch};
use lance::Dataset;
use log::{debug, info};
use std::path::PathBuf;
use std::sync::Arc;

use crate::datasets::path_to_uri;
use crate::display::display::display_spreadsheet_interactive;
use crate::functions::functions::normalize_for_display;

/// Randomly sample `n_rows` rows from a Lance dataset and show them
/// in the interactive spreadsheet viewer.
///
/// - supports all layouts; dense row‑major vectors are expanded before viewing.
pub async fn cmd_sample(filepath: &PathBuf, n_rows: usize) -> Result<()> {
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use rand::rng;
    use rand::seq::SliceRandom;

    info!(
        "cmd_sample: requested {} random rows from {:?}",
        n_rows, filepath
    );
    println!("=== {} random samples (interactive) ===", n_rows);

    // Canonicalize the path so logs and Lance see a stable URI.
    let uri = path_to_uri(filepath);
    debug!("cmd_sample: opening dataset at URI {}", uri);

    let dataset = Dataset::open(&uri).await?;

    // Count total rows once up front.
    let total_rows = dataset.count_rows(None).await?;
    info!("cmd_sample: dataset has {} rows", total_rows);

    if total_rows == 0 {
        println!("Dataset is empty");
        return Ok(());
    }

    // Clamp requested rows to dataset size.
    let n = n_rows.min(total_rows);
    info!("cmd_sample: effective sample size {}", n);

    // Generate and shuffle indices [0, total_rows).
    let mut rng = rng();
    let mut indices: Vec<i64> = (0..total_rows as i64).collect();
    indices.shuffle(&mut rng);
    indices.truncate(n);
    indices.sort_unstable(); // so max_index is last

    debug!(
        "cmd_sample: first 10 sampled indices: {:?}",
        &indices[..indices.len().min(10)]
    );
    println!("Sampling {} rows from {} total", indices.len(), total_rows);

    // Read prefix [0, max_index] as a batch, then gather only sampled rows.
    let max_index = *indices.last().unwrap();
    debug!(
        "cmd_sample: max sampled index {}, reading prefix [0, {}]",
        max_index,
        max_index + 1
    );

    let mut scanner = dataset.scan();
    let batch = scanner
        .limit(Some(max_index + 1), None)?
        .try_into_batch()
        .await?;
    debug!(
        "cmd_sample: loaded prefix batch with {} rows × {} cols",
        batch.num_rows(),
        batch.num_columns()
    );

    if batch.num_rows() == 0 {
        println!("No data to display");
        return Ok(());
    }

    // Build an Arrow index array to take the sampled rows from the prefix batch.
    let index_array = Arc::new(arrow::array::Int64Array::from(indices.clone())) as ArrayRef;
    let mut sampled_columns = Vec::with_capacity(batch.num_columns());

    for (i, col) in batch.columns().iter().enumerate() {
        debug!("cmd_sample: taking sampled rows for column {}", i);
        let taken = take(col.as_ref(), &index_array, None)?;
        sampled_columns.push(Arc::from(taken));
    }

    // Add explicit original row index column as first column.
    let original_idx_array =
        UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<u32>>());
    let original_idx_col: ArrayRef = Arc::new(original_idx_array);

    let old_schema = batch.schema();
    let mut new_fields: Vec<Field> = Vec::with_capacity(old_schema.fields().len() + 1);
    new_fields.push(Field::new("row_idx", DataType::UInt32, false));
    for f in old_schema.fields() {
        // f: &Arc<Field> -> &Field via Deref, then clone Field
        new_fields.push((**f).clone());
    }
    let new_schema = Arc::new(Schema::new(new_fields));

    let mut new_columns = Vec::with_capacity(sampled_columns.len() + 1);
    new_columns.push(original_idx_col);
    new_columns.extend(sampled_columns);

    let sampled_batch = RecordBatch::try_new(new_schema, new_columns)?;
    info!(
        "cmd_sample: built sampled batch with {} rows × {} cols",
        sampled_batch.num_rows(),
        sampled_batch.num_columns()
    );

    if sampled_batch.num_rows() == 0 {
        println!("No sampled data to display");
        return Ok(());
    }

    let sampled_batch = normalize_for_display(&sampled_batch)?;
    debug!("cmd_sample: launching interactive viewer for sampled batch");
    display_spreadsheet_interactive(&sampled_batch)?;
    Ok(())
}
