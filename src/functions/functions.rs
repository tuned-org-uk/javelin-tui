use anyhow::anyhow;
use anyhow::{Context, Result};

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Array as ArrowArray, ArrayRef, FixedSizeListArray, Float64Array, RecordBatch};
use std::path::PathBuf;
use std::sync::Arc;

use crate::display::LanceLayout;

/// Detect the Lance layout type from a RecordBatch schema.
pub(crate) fn detect_lance_layout(batch: &RecordBatch) -> LanceLayout {
    let schema = batch.schema();
    let fields = schema.fields();

    // Sparse COO: row/col/value with expected types
    if fields.len() == 3 {
        let names: Vec<_> = fields.iter().map(|f| f.name().as_str()).collect();
        if names == ["row", "col", "value"] {
            return LanceLayout::SparseCoo;
        }
    }

    // Single-column cases: dense row-major or 1D vector
    if fields.len() == 1 {
        let f = &fields[0];
        match f.data_type() {
            DataType::FixedSizeList(inner, _) => {
                if matches!(inner.data_type(), DataType::Float64) {
                    return LanceLayout::DenseRowMajor;
                }
            }
            DataType::Float64
            | DataType::Int64
            | DataType::UInt32
            | DataType::Int32
            | DataType::UInt64
            | DataType::Int16
            | DataType::UInt16
            | DataType::Int8
            | DataType::UInt8 => {
                return LanceLayout::Vector1D;
            }
            _ => panic!("Single column file, datatype not recognised {:?}", f),
        }
    }

    LanceLayout::Other
}

/// Expand a dense row‑major FixedSizeList<Float64> column into scalar Float64
/// columns col_0, col_1, ..., col_(F-1) for nicer display and sampling.
///
/// Input schema:  { vector: FixedSizeList<Float64>[F] }
/// Output schema: { col_0: Float64, ..., col_(F-1): Float64 }
pub(crate) fn expand_dense_row_major(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_columns() != 1 {
        return Err(anyhow!(
            "expand_dense_row_major: expected 1 column, got {}",
            batch.num_columns()
        ));
    }

    let col = batch.column(0);
    let list = col
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .context("expand_dense_row_major: expected FixedSizeList column")?;

    let n_rows = list.len();
    let width = list.value_length() as usize;

    let values = list
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("expand_dense_row_major: values must be Float64")?;

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(width);
    let mut fields: Vec<Field> = Vec::with_capacity(width);

    for dim in 0..width {
        let data: Vec<f64> = (0..n_rows)
            .map(|r| {
                // Row‑major index into the underlying values array
                let idx = r * width + dim;
                values.value(idx)
            })
            .collect();

        cols.push(Arc::new(Float64Array::from(data)) as ArrayRef);
        fields.push(Field::new(&format!("col_{dim}"), DataType::Float64, false));
    }

    let schema = Arc::new(Schema::new(fields));
    let out = RecordBatch::try_new(schema, cols)?;
    Ok(out)
}

/// Normalize a RecordBatch into a form suitable for display / sampling:
///
/// - DenseRowMajor → expanded scalar columns
/// - SparseCoo, Vector1D, Other → returned unchanged
pub(crate) fn normalize_for_display(batch: &RecordBatch) -> Result<RecordBatch> {
    match detect_lance_layout(batch) {
        LanceLayout::DenseRowMajor => expand_dense_row_major(batch),
        LanceLayout::SparseCoo | LanceLayout::Vector1D | LanceLayout::Other => Ok(batch.clone()),
    }
}

#[allow(dead_code)]
fn cmd_clusters(filepath: &PathBuf) -> Result<()> {
    println!("=== Cluster Information ===");
    println!("Filepath: {}", filepath.display());
    println!("\n[Cluster visualization would appear here]");

    // Example placeholder for future integration with ArrowSpace metadata.

    Ok(())
}
