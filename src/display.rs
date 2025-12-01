use anyhow::{anyhow, bail, Result};
use arrow::array::*;
use arrow::datatypes::DataType;
use arrow_array::{ArrayRef, RecordBatch};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::text::Span;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph, Row, Table},
    Frame, Terminal,
};
use std::io;

use crate::{
    display_1d::render_1d_ui, display_transposed::render_transposed_ui, functions::LanceLayout,
};

// === Public entry point =====================================================

/// Launch an interactive spreadsheet-like TUI for a Lance `RecordBatch`.
///
/// # Arguments
/// * `batch` - Arrow `RecordBatch` containing a header row with metadata
///   (`name_id`, `n_rows`, `n_cols`) and feature columns named `col_*`.
///
/// The function:
/// - scans the schema once to find all feature columns (`col_*`),
/// - opens a ratatui / crossterm alternate screen,
/// - lets the user scroll horizontally over feature columns,
/// - and exits when the user presses `q` or `Esc`.
pub fn display_spreadsheet_interactive(batch: &RecordBatch) -> Result<()> {
    use log::{debug, info};

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let layout = crate::functions::detect_lance_layout(batch);

    info!(
        "display_spreadsheet_interactive: starting viewer for batch (rows={}, cols={})",
        num_rows, num_cols
    );

    if num_cols == 0 {
        println!("No columns to display");
        info!("display_spreadsheet_interactive: abort, no columns");
        return Err(anyhow!(
            "display_spreadsheet_interactive: abort, no columns"
        ));
    }

    // Discover all feature columns once (col_*)
    let all_col_indices = collect_feature_cols(batch)?;
    info!(
        "display_spreadsheet_interactive: found {} feature columns (col_*)",
        all_col_indices.len()
    );

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut col_offset: usize = 0; // horizontal scroll over features (N×F)
    let mut row_offset: usize = 0; // horizontal scroll over rows (F×N)
    let mut row_start: usize = 0; // vertical scroll (top visible row / feature)
    let visible: usize = 8; // number of visible items horizontally
    let mut transposed = false; // false = N×F, true = F×N

    info!(
        "display_spreadsheet_interactive: initial state mode=N×F, visible={}, offsets=(col=0,row=0,start=0)",
        visible
    );

    loop {
        terminal.draw(|f| match layout {
            LanceLayout::SparseCoo => crate::display_coo::render_coo_ui(f, batch, row_start),
            LanceLayout::Vector1D => {
                render_1d_ui(
                    f,
                    batch,
                    &all_col_indices,
                    col_offset,
                    visible,
                    num_rows,
                    num_cols,
                    row_start,
                );
            }
            _ => {
                if transposed {
                    render_transposed_ui(
                        f,
                        batch,
                        &all_col_indices,
                        row_offset,
                        visible,
                        num_rows,
                        num_cols,
                        row_start,
                    );
                } else {
                    render_base_ui(
                        f,
                        batch,
                        &all_col_indices,
                        col_offset,
                        visible,
                        num_rows,
                        num_cols,
                        row_start,
                    );
                }
            }
        })?;

        // clamp horizontal offsets
        if transposed {
            let max_row_off = num_rows.saturating_sub(visible);
            if row_offset > max_row_off {
                debug!(
                    "display_spreadsheet_interactive: clamp row_offset {} -> {}",
                    row_offset, max_row_off
                );
                row_offset = max_row_off;
            }
        } else {
            let max_col_off = all_col_indices.len().saturating_sub(visible);
            if col_offset > max_col_off {
                debug!(
                    "display_spreadsheet_interactive: clamp col_offset {} -> {}",
                    col_offset, max_col_off
                );
                col_offset = max_col_off;
            }
        }

        // clamp vertical offset
        let max_row_start = num_rows.saturating_sub(1);
        if row_start > max_row_start {
            debug!(
                "display_spreadsheet_interactive: clamp row_start {} -> {}",
                row_start, max_row_start
            );
            row_start = max_row_start;
        }

        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(KeyEvent { code, .. }) = event::read()? {
                match code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        info!("display_spreadsheet_interactive: user quit (q/ESC)");
                        break;
                    }

                    KeyCode::Char('t') => {
                        transposed = !transposed;
                        col_offset = 0;
                        row_offset = 0;
                        row_start = 0;
                        info!(
                            "display_spreadsheet_interactive: toggle transpose -> mode={} (N×F=false,F×N=true)",
                            transposed
                        );
                    }

                    // horizontal right
                    KeyCode::Right | KeyCode::Char('l') => {
                        if transposed {
                            let max = num_rows.saturating_sub(visible);
                            if row_offset < max {
                                row_offset += 1;
                                debug!(
                                    "display_spreadsheet_interactive: row_offset -> {} (F×N, →)",
                                    row_offset
                                );
                            }
                        } else {
                            let max = all_col_indices.len().saturating_sub(visible);
                            if col_offset < max {
                                col_offset += 1;
                                debug!(
                                    "display_spreadsheet_interactive: col_offset -> {} (N×F, →)",
                                    col_offset
                                );
                            }
                        }
                    }

                    // horizontal left
                    KeyCode::Left | KeyCode::Char('h') => {
                        if transposed {
                            if row_offset > 0 {
                                row_offset -= 1;
                                debug!(
                                    "display_spreadsheet_interactive: row_offset -> {} (F×N, ←)",
                                    row_offset
                                );
                            }
                        } else if col_offset > 0 {
                            col_offset -= 1;
                            debug!(
                                "display_spreadsheet_interactive: col_offset -> {} (N×F, ←)",
                                col_offset
                            );
                        }
                    }

                    // jump first/last horizontally
                    KeyCode::Char('H') => {
                        if transposed {
                            row_offset = 0;
                            debug!("display_spreadsheet_interactive: row_offset -> 0 (H)");
                        } else {
                            col_offset = 0;
                            debug!("display_spreadsheet_interactive: col_offset -> 0 (H)");
                        }
                    }
                    KeyCode::Char('E') => {
                        if transposed {
                            row_offset = num_rows.saturating_sub(visible);
                            debug!(
                                "display_spreadsheet_interactive: row_offset -> {} (E)",
                                row_offset
                            );
                        } else {
                            col_offset = all_col_indices.len().saturating_sub(visible);
                            debug!(
                                "display_spreadsheet_interactive: col_offset -> {} (E)",
                                col_offset
                            );
                        }
                    }

                    // vertical scroll
                    KeyCode::Up | KeyCode::Char('k') => {
                        if row_start > 0 {
                            row_start -= 1;
                            debug!(
                                "display_spreadsheet_interactive: row_start -> {} (↑/k)",
                                row_start
                            );
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if row_start < max_row_start {
                            row_start += 1;
                            debug!(
                                "display_spreadsheet_interactive: row_start -> {} (↓/j)",
                                row_start
                            );
                        }
                    }

                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    info!("display_spreadsheet_interactive: terminal restored, exiting viewer");
    Ok(())
}

// === Formatting helpers =====================================================

/// Format a single value from an Arrow array at the given row index as a string.
///
/// # Arguments
/// * `array` - Arrow `ArrayRef` representing one column.
/// * `row_idx` - Zero-based row index to read from `array`.
///
/// The function:
/// - handles basic numeric, boolean, and UTF-8 string types,
/// - returns `"NULL"` for null entries,
/// - truncates long UTF-8 strings to 10 characters with an ellipsis.
fn format_value(array: &ArrayRef, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            format!("{:.8}", arr.value(row_idx))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{:.8}", arr.value(row_idx))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            format!("{}", arr.value(row_idx))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            format!("{}", arr.value(row_idx))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            format!("{}", arr.value(row_idx))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            format!("{}", arr.value(row_idx))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.value(row_idx) { "true" } else { "false" }.to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let s = arr.value(row_idx);
            if s.len() > 10 {
                format!("{}…", &s[0..9])
            } else {
                s.to_string()
            }
        }
        _ => "?".to_string(),
    }
}

// === Column selection / windows ============================================

/// Collect the indices of all feature columns used by the viewer.
///
/// Primary mode:
///   - columns whose names start with `col_` (dense feature matrices).
///
/// Fallbacks when no `col_*` columns exist:
///   - if there is exactly one column, treat it as a single feature
///     (1D vectors like lambdas, centroid_map, norms, etc.);
///   - otherwise, treat all numeric columns as features
///     (e.g. sparse COO {row, col, value}).
fn collect_feature_cols(batch: &RecordBatch) -> Result<Vec<usize>> {
    let schema = batch.schema();

    // 1) Preferred: explicit `col_*` feature columns
    let mut cols: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            if f.name().starts_with("col_") {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if !cols.is_empty() {
        return Ok(cols);
    }

    // 2) Fallback for 1D vectors: single column => treat as one feature
    if batch.num_columns() == 1 {
        return Ok(vec![0]);
    }

    // 3) Fallback for generic numeric tables (e.g. sparse COO row/col/value):
    //    use all numeric columns as features.
    cols = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| match f.data_type() {
            DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(i),
            _ => None,
        })
        .collect();

    if cols.is_empty() {
        bail!(
            "The file should be formatted with `col_*` feature columns \
             or at least one numeric column; got schema {:?}",
            schema
        );
    }

    Ok(cols)
}

/// Compute a sliding window over the feature column indices for horizontal scrolling.
///
/// # Arguments
/// * `all_cols` - Slice of all `col_*` indices in schema order.
/// * `col_offset` - Current horizontal offset (starting column in `all_cols`).
/// * `visible_cols` - Maximum number of feature columns that can be shown.
///
/// # Returns
/// A subslice of `all_cols` representing the currently visible feature columns.
fn feature_window<'a>(
    all_cols: &'a [usize],
    col_offset: usize,
    visible_cols: usize,
) -> &'a [usize] {
    let start = col_offset.min(all_cols.len());
    let end = (start + visible_cols).min(all_cols.len());
    &all_cols[start..end]
}

// === Header / rows =========================================================

/// Build the table header row for the current feature window.
///
/// # Arguments
/// * `batch` - Arrow `RecordBatch` providing schema information for column names.
/// * `col_window` - Slice of feature column indices to display.
///
/// The header contains:
/// - a leading `"Row"` column,
/// - one column per `col_*` feature in `col_window`,
/// - two trailing columns `"avg"` and `"std"` for per-row statistics.
fn render_header<'a>(batch: &'a RecordBatch, col_window: &'a [usize]) -> Row<'a> {
    let schema = batch.schema();
    let mut header_cells = vec!["Row".to_string()];
    for &i in col_window {
        header_cells.push(schema.field(i).name().to_string());
    }
    header_cells.push("avg".to_string());
    header_cells.push("std".to_string());

    Row::new(header_cells)
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .height(1)
}

// === UI ====================================================================

/// Render the full TUI layout for a single frame.
///
/// # Arguments
/// * `f` - Mutable ratatui `Frame` used for drawing widgets.
/// * `batch` - Arrow `RecordBatch` backing the table view.
/// * `all_col_indices` - Slice of all `col_*` feature column indices.
/// * `col_offset` - Current horizontal offset into `all_col_indices`.
/// * `visible_cols` - Maximum number of feature columns to show at once.
/// * `num_rows` - Total number of rows in `batch`.
/// * `num_cols` - Total number of columns in `batch` (including metadata).
///
/// Layout:
/// - Top: metadata block showing `name_id`, `n_rows`, `n_cols` (if available).
/// - Middle: main table with row id, feature columns, and avg/std per row.
/// - Bottom: status bar with dimensions and key bindings.
fn render_base_ui(
    f: &mut Frame,
    batch: &RecordBatch,
    all_col_indices: &[usize],
    col_offset: usize,
    visible_cols: usize,
    num_rows: usize,
    num_cols: usize,
    row_start: usize,
) {
    // 1) Split into metadata / table / status, same as transposed
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // metadata
            Constraint::Min(0),    // table
            Constraint::Length(3), // status
        ])
        .split(f.area());

    let schema = batch.schema();

    // ---- Metadata header (same logic you already use in transposed) ----
    let mut name_idx = None;
    let mut n_rows_idx = None;
    let mut n_cols_idx = None;
    for (i, field) in schema.fields().iter().enumerate() {
        match field.name().as_str() {
            "name_id" => name_idx = Some(i),
            "n_rows" => n_rows_idx = Some(i),
            "n_cols" => n_cols_idx = Some(i),
            _ => {}
        }
    }

    let meta_text = if let Some(name_i) = name_idx {
        let name = format_value(batch.column(name_i), 0);
        let nrows_val = n_rows_idx
            .map(|i| format_value(batch.column(i), 0))
            .unwrap_or_else(|| "?".to_string());
        let ncols_val = n_cols_idx
            .map(|i| format_value(batch.column(i), 0))
            .unwrap_or_else(|| "?".to_string());
        format!("name_id: {name}    n_rows: {nrows_val}    n_cols: {ncols_val}")
    } else {
        format!("rows: {num_rows}    cols: {num_cols}")
    };

    let header_paragraph = Paragraph::new(Span::raw(meta_text))
        .block(Block::default().borders(Borders::ALL).title(" Metadata "));
    f.render_widget(header_paragraph, chunks[0]);

    // ---- Determine vertical window for table rows based on chunks[1].height ----
    let table_area_height = chunks[1].height.saturating_sub(3); // header row + borders
    let max_visible_rows = table_area_height as usize;
    let end_row = (row_start + max_visible_rows).min(num_rows);

    // ---- Horizontal window over features, as before ----
    let col_window = feature_window(all_col_indices, col_offset, visible_cols);
    let header_row = render_header(batch, col_window);

    // Render only rows [row_start, end_row)
    let rows = render_rows_window(batch, col_window, all_col_indices, row_start, end_row);

    let mut widths = vec![Constraint::Length(5)]; // "Row" column
    for _ in col_window {
        widths.push(Constraint::Length(12));
    }
    widths.push(Constraint::Length(10)); // avg
    widths.push(Constraint::Length(10)); // std

    let total_feat_cols = all_col_indices.len();
    let start_col = if total_feat_cols == 0 {
        0
    } else {
        col_offset + 1
    };
    let end_col = (col_offset + col_window.len()).min(total_feat_cols);

    let title = format!(
        " Lance Data (rows {}–{} of {}, feature cols {}–{} of {}) ",
        row_start + 1,
        end_row,
        num_rows,
        start_col,
        end_col,
        total_feat_cols
    );

    let table = Table::new(rows, widths)
        .header(header_row)
        .block(Block::default().borders(Borders::ALL).title(title))
        .column_spacing(1);

    f.render_widget(table, chunks[1]);

    // ---- Status bar at bottom ----
    let status = format!(
        " {} rows × {} total cols | {} feature cols (col_*) | mode: N×F | ↑↓ scroll rows | ←→ scroll features | t transpose | q quit ",
        num_rows, num_cols, total_feat_cols
    );
    let status_widget = Block::default().borders(Borders::ALL).title(status);
    f.render_widget(status_widget, chunks[2]);
}

fn render_rows_window<'a>(
    batch: &'a RecordBatch,
    col_window: &'a [usize],
    all_cols: &'a [usize],
    row_start: usize,
    row_end: usize,
) -> Vec<Row<'a>> {
    let mut out = Vec::with_capacity(row_end.saturating_sub(row_start));

    for row_idx in row_start..row_end {
        let mut cells = vec![row_idx.to_string()];

        // visible feature values
        for &col_idx in col_window {
            let col = batch.column(col_idx);
            let s = format_value(col, row_idx);
            cells.push(s);
        }

        // stats over ALL features (unchanged from your existing render_rows)
        let mut vals: Vec<f64> = Vec::with_capacity(all_cols.len());
        for &col_idx in all_cols {
            let col = batch.column(col_idx);
            if col.is_null(row_idx) {
                continue;
            }
            match col.data_type() {
                DataType::Float32 => {
                    let a = col.as_any().downcast_ref::<Float32Array>().unwrap();
                    vals.push(a.value(row_idx) as f64);
                }
                DataType::Float64 => {
                    let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    vals.push(a.value(row_idx));
                }
                DataType::Int32 => {
                    let a = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    vals.push(a.value(row_idx) as f64);
                }
                DataType::Int64 => {
                    let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    vals.push(a.value(row_idx) as f64);
                }
                DataType::UInt32 => {
                    let a = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                    vals.push(a.value(row_idx) as f64);
                }
                DataType::UInt64 => {
                    let a = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                    vals.push(a.value(row_idx) as f64);
                }
                _ => {}
            }
        }

        let (avg_str, std_str) = if vals.is_empty() {
            ("NA".to_string(), "NA".to_string())
        } else {
            let n = vals.len() as f64;
            let sum: f64 = vals.iter().sum();
            let mean = sum / n;
            let var: f64 = vals.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n;
            let std = var.sqrt();
            (format!("{:.4}", mean), format!("{:.4}", std))
        };

        cells.push(avg_str);
        cells.push(std_str);

        out.push(Row::new(cells).height(1));
    }

    out
}
