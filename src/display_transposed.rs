use arrow::array::*;
use arrow::datatypes::DataType;
use arrow_array::{ArrayRef, RecordBatch};
use ratatui::text::Span;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph, Row, Table},
};

/// Render one frame of the transposed F×N view (features × rows).
///
/// # Arguments
/// * `f`          - ratatui frame to draw into.
/// * `batch`      - underlying RecordBatch.
/// * `all_cols`   - all `col_*` feature column indices (schema order).
/// * `row_offset` - horizontal offset over rows (scrolling dimension).
/// * `visible`    - maximum number of rows to show as columns.
/// * `num_rows`   - total number of rows in `batch`.
/// * `num_cols`   - total number of columns in `batch`, including metadata.
pub fn render_transposed_ui(
    f: &mut Frame,
    batch: &RecordBatch,
    all_cols: &[usize],
    row_offset: usize,
    visible: usize,
    num_rows: usize,
    num_cols: usize,
    row_start: usize, // NEW: top feature index
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(f.area());

    let meta_text = build_metadata_line(batch, num_rows, num_cols);
    let header_paragraph = Paragraph::new(Span::raw(meta_text))
        .block(Block::default().borders(Borders::ALL).title(" Metadata "));
    f.render_widget(header_paragraph, chunks[0]);

    // compute visible feature rows based on terminal height
    let data_area_height = chunks[1].height.saturating_sub(3);
    let max_visible_feats = data_area_height as usize;
    let total_feats = all_cols.len();
    let feat_start = row_start.min(total_feats);
    let feat_end = (feat_start + max_visible_feats).min(total_feats);
    let feat_window = &all_cols[feat_start..feat_end];

    let row_window = row_window(num_rows, row_offset, visible);
    let header_row = render_header_transposed(&row_window);
    let rows = render_rows_transposed_window(batch, &row_window, feat_window);

    let mut widths = vec![Constraint::Length(10)];
    for _ in &row_window {
        widths.push(Constraint::Length(10));
    }
    widths.push(Constraint::Length(10));
    widths.push(Constraint::Length(10));

    let start_r = if num_rows == 0 { 0 } else { row_offset + 1 };
    let end_r = (row_offset + row_window.len()).min(num_rows);

    let title = format!(
        " Lance Data (features {}–{} of {}, rows {}–{} of {}) ",
        feat_start + 1,
        feat_end,
        total_feats,
        start_r,
        end_r,
        num_rows
    );

    let table = Table::new(rows, widths)
        .header(header_row)
        .block(Block::default().borders(Borders::ALL).title(title))
        .column_spacing(1);

    f.render_widget(table, chunks[1]);

    let status = format!(
        " {} rows × {} total cols | {} feature cols (col_*) | mode: F×N | ↑↓ scroll features | ←→ scroll rows | t transpose | q quit ",
        num_rows, num_cols, total_feats,
    );
    let status_widget = Block::default().borders(Borders::ALL).title(status);
    f.render_widget(status_widget, chunks[2]);
}

fn render_rows_transposed_window<'a>(
    batch: &'a RecordBatch,
    row_window: &'a [usize],
    feat_window: &'a [usize],
) -> Vec<Row<'a>> {
    let mut out = Vec::with_capacity(feat_window.len());

    for &col_idx in feat_window {
        let col = batch.column(col_idx);
        let name = batch.schema().field(col_idx).name().to_string();
        let mut cells = vec![name];

        for &row_idx in row_window {
            cells.push(format_value(col, row_idx));
        }

        // stats over all rows (same as existing render_rows_transposed)
        let mut vals: Vec<f64> = Vec::new();
        let n_rows = batch.num_rows();
        for row_idx in 0..n_rows {
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

/// Build the metadata line, shared with the non-transposed view.
fn build_metadata_line(batch: &RecordBatch, num_rows: usize, num_cols: usize) -> String {
    let schema = batch.schema();
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

    if let Some(name_i) = name_idx {
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
    }
}

/// Compute visible row indices for transposed mode.
fn row_window(total_rows: usize, row_offset: usize, visible: usize) -> Vec<usize> {
    let start = row_offset.min(total_rows);
    let end = (start + visible).min(total_rows);
    (start..end).collect()
}

/// Header for transposed mode: "Feature", one column per row, then avg/std.
fn render_header_transposed(row_window: &[usize]) -> Row<'_> {
    let mut cells = vec!["Feature".to_string()];
    for &r in row_window {
        cells.push(format!("row_{r}"));
    }
    cells.push("avg".to_string());
    cells.push("std".to_string());
    Row::new(cells)
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .height(1)
}

/// Reuse the same formatter from display.rs; keep in sync with it.
fn format_value(array: &ArrayRef, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return "NULL".to_string();
    }
    match array.data_type() {
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            format!("{:.4}", arr.value(row_idx))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{:.4}", arr.value(row_idx))
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
