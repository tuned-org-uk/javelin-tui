use anyhow::anyhow;
use anyhow::{Context, Result};

use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Array as ArrowArray, ArrayRef, FixedSizeListArray, Float64Array, RecordBatch};
use lance::dataset::Dataset;
use log::{debug, info};
use rand::seq::SliceRandom;
use std::path::PathBuf;
use std::sync::Arc;

use crate::display::*;

/// Logical view of how a Lance dataset is stored.
///
/// - DenseRowMajor: { vector: FixedSizeList<Float64>[F] } – each row is a dense vector
/// - SparseCoo:     { row: UInt32, col: UInt32, value: Float64 } – COO triplets
/// - Vector1D:      single primitive column (e.g. lambdas, norms, indices)
/// - Other:         anything else; shown as‑is
pub enum LanceLayout {
    DenseRowMajor,
    SparseCoo,
    Vector1D,
    Other,
}

/// Detect the Lance layout type from a RecordBatch schema.
pub fn detect_lance_layout(batch: &RecordBatch) -> LanceLayout {
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
fn expand_dense_row_major(batch: &RecordBatch) -> Result<RecordBatch> {
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
fn normalize_for_display(batch: &RecordBatch) -> Result<RecordBatch> {
    match detect_lance_layout(batch) {
        LanceLayout::DenseRowMajor => expand_dense_row_major(batch),
        LanceLayout::SparseCoo | LanceLayout::Vector1D | LanceLayout::Other => Ok(batch.clone()),
    }
}

pub async fn cmd_info(filepath: &PathBuf) -> Result<()> {
    println!("=== Lance File Info ===");
    println!("Path: {}", filepath.display());

    // Open the Lance dataset
    let uri = format!("file://{}", filepath.canonicalize()?.display());
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

fn cmd_files(filepath: &PathBuf) -> Result<()> {
    println!("=== Files in Lance Dataset ===");
    println!("Base path: {}", filepath.display());

    if filepath.is_dir() {
        println!("\nDirectory contents:");
        for entry in std::fs::read_dir(filepath)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            println!(
                " {} - {} bytes",
                entry.file_name().to_string_lossy(),
                metadata.len()
            );
        }
    } else {
        println!("Single file: {} bytes", filepath.metadata()?.len());
    }

    Ok(())
}

pub async fn cmd_head(filepath: &PathBuf, n: usize) -> Result<()> {
    let uri = format!("file://{}", filepath.canonicalize()?.display());
    let dataset = Dataset::open(&uri).await?;
    let mut scanner = dataset.scan();

    let batch = scanner
        .limit(Some(n as i64), None)?
        .try_into_batch()
        .await?;

    if batch.num_rows() == 0 {
        println!("No data to display");
        return Ok(());
    }

    let batch = normalize_for_display(&batch)?;
    display_spreadsheet_interactive(&batch)?;
    Ok(())
}

pub async fn cmd_display(filepath: &PathBuf) -> Result<()> {
    info!("cmd_display: opening full dataset at {:?}", filepath);

    let abs = filepath.canonicalize()?;
    let uri = format!("file://{}", abs.display());
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

/// Randomly sample `n_rows` rows from a Lance dataset and show them
/// in the interactive spreadsheet viewer.
///
/// - supports all layouts; dense row‑major vectors are expanded before viewing.
pub async fn cmd_sample(filepath: &PathBuf, n_rows: usize) -> Result<()> {
    info!(
        "cmd_sample: requested {} random rows from {:?}",
        n_rows, filepath
    );
    println!("=== {} random samples (interactive) ===", n_rows);

    // Canonicalize the path so logs and Lance see a stable URI.
    let abs = filepath.canonicalize()?;
    let uri = format!("file://{}", abs.display());
    debug!("cmd_sample: opening dataset at URI {}", uri);

    let dataset = Dataset::open(&uri).await?;

    // Count total rows once up front; this hits metadata and is cheap.
    let total_rows = dataset.count_rows(None).await?;
    info!("cmd_sample: dataset has {} rows", total_rows);

    if total_rows == 0 {
        println!("Dataset is empty");
        return Ok(());
    }

    // Defensive check: n_rows must not exceed the dataset length.
    // Using assert! here will panic in debug; you may prefer a fallible check.
    assert!(
        total_rows >= n_rows,
        "n_rows exceeds dataset length: {} > {}",
        n_rows,
        total_rows
    );

    // Clamp requested rows to dataset size (in case of equality).
    let n = n_rows.min(total_rows);
    info!("cmd_sample: effective sample size {}", n);

    // Generate a vector of row indices [0, total_rows) and shuffle in place.
    let mut rng = rand::rng();
    let mut indices: Vec<i64> = (0..total_rows as i64).collect();
    indices.shuffle(&mut rng);
    indices.truncate(n);
    indices.sort_unstable(); // important so max_index is last
    debug!(
        "cmd_sample: first 10 sampled indices: {:?}",
        &indices[..indices.len().min(10)]
    );
    println!("Sampling {} rows from {} total", indices.len(), total_rows);

    // For simplicity, read a contiguous prefix [0, max_index] as a batch,
    // then use Arrow `take` to gather only the sampled indices.
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

    // Build an Arrow index array to "take" the sampled rows from the prefix batch.
    let index_array = Arc::new(arrow::array::Int64Array::from(indices.clone())) as ArrayRef;
    let mut sampled_columns = Vec::with_capacity(batch.num_columns());

    for (i, col) in batch.columns().iter().enumerate() {
        debug!("cmd_sample: taking sampled rows for column {}", i);
        let taken = take(col.as_ref(), &index_array, None)?;
        sampled_columns.push(Arc::from(taken));
    }

    let sampled_batch = RecordBatch::try_new(batch.schema(), sampled_columns)?;
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
    // Hand off to the interactive spreadsheet viewer.
    debug!("cmd_sample: launching interactive viewer for sampled batch");
    display_spreadsheet_interactive(&sampled_batch)?;
    Ok(())
}

pub async fn cmd_stats(filepath: &PathBuf) -> Result<()> {
    println!("=== Dataset Statistics ===");

    let uri = format!("file://{}", filepath.canonicalize()?.display());
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

fn cmd_plot_lambdas(filepath: &PathBuf, bins: usize) -> Result<()> {
    println!("=== Lambda Distribution (bins: {}) ===", bins);
    println!("Filepath: {}", filepath.display());
    println!("\n[Histogram visualization would appear here]");
    println!("(Requires trueno-viz integration)");

    // Example placeholder for future integration:
    // let lambdas = load_lambdas_from_lance(filepath)?;
    // build histogram with trueno-viz...

    Ok(())
}

fn cmd_plot_laplacian(filepath: &PathBuf, mode: &str) -> Result<()> {
    println!("=== Laplacian Plot (mode: {}) ===", mode);
    println!("Filepath: {}", filepath.display());
    println!("\n[Laplacian visualization would appear here]");
    println!("Mode: {}", mode);

    // Example placeholder for future integration with sprs visualisation utilities.

    Ok(())
}

fn cmd_clusters(filepath: &PathBuf) -> Result<()> {
    println!("=== Cluster Information ===");
    println!("Filepath: {}", filepath.display());
    println!("\n[Cluster visualization would appear here]");

    // Example placeholder for future integration with ArrowSpace metadata.

    Ok(())
}

pub async fn run_tui(filepath: PathBuf) -> Result<()> {
    use crossterm::{
        ExecutableCommand,
        event::{self, Event, KeyCode},
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    };
    use ratatui::{
        Terminal,
        backend::CrosstermBackend,
        layout::{Constraint, Direction, Layout},
        widgets::{Block, Borders, Paragraph},
    };
    use std::io::stdout;

    // Setup terminal
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    loop {
        terminal.draw(|frame| {
            let size = frame.area();

            // Simple layout: header + content
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0)])
                .split(size);

            // Header
            let header = Paragraph::new(format!(
                "Javelin - Lance Inspector\nFile: {}",
                filepath.display()
            ))
            .block(Block::default().borders(Borders::ALL).title("Info"));
            frame.render_widget(header, chunks[0]);

            // Content area
            let content = Paragraph::new("Press 'q' to quit\nTUI content would go here")
                .block(Block::default().borders(Borders::ALL).title("Content"));
            frame.render_widget(content, chunks[1]);
        })?;

        // Handle events
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    // Cleanup
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
