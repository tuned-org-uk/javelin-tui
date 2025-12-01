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

/// Render a 1D vector dataset (LanceLayout::Vector1D).
///
/// Layout:
/// - Top: metadata (same style as main viewer)
/// - Middle: table with `Row | value` (no avg/std), 12 decimal digits for floats
/// - Bottom: status bar
pub fn render_1d_ui(
    f: &mut Frame,
    batch: &RecordBatch,
    col_indices: &[usize],
    col_offset: usize,
    visible_cols: usize,
    num_rows: usize,
    num_cols: usize,
    row_start: usize,
) {
    // 1) Split into metadata / table / status
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // metadata
            Constraint::Min(0),    // table
            Constraint::Length(3), // status
        ])
        .split(f.area());

    let schema = batch.schema();

    // ---- Metadata header (copied from display.rs base UI) ----
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
        format!("name_id: {name} n_rows: {nrows_val} n_cols: {ncols_val}")
    } else {
        format!("rows: {num_rows} cols: {num_cols}")
    };

    let header_paragraph = Paragraph::new(Span::raw(meta_text))
        .block(Block::default().borders(Borders::ALL).title(" Metadata "));
    f.render_widget(header_paragraph, chunks[0]);

    // ---- Row window / column window ----
    let table_area_height = chunks[1].height.saturating_sub(3);
    let max_visible_rows = table_area_height as usize;
    let end_row = (row_start + max_visible_rows).min(num_rows);

    let col_window = feature_window(col_indices, col_offset, visible_cols);

    // ---- Header + rows ----
    let header_row = render_header_1d(batch, col_window);
    let rows = render_rows_window_1d(batch, col_window, row_start, end_row);

    let mut widths = vec![Constraint::Length(5)]; // "Row"
    for _ in col_window {
        widths.push(Constraint::Length(26)); // enough for 12 decimal digits
    }

    let total_feat_cols = col_indices.len();
    let start_col = if total_feat_cols == 0 {
        0
    } else {
        col_offset + 1
    };
    let end_col = (col_offset + col_window.len()).min(total_feat_cols);

    let title = format!(
        " Lance Vector Data (rows {}–{} of {}, cols {}–{} of {}) ",
        row_start + 1,
        end_row,
        num_rows,
        start_col,
        end_col,
        total_feat_cols,
    );

    let table = Table::new(rows, widths)
        .header(header_row)
        .block(Block::default().borders(Borders::ALL).title(title))
        .column_spacing(1);
    f.render_widget(table, chunks[1]);

    // ---- Status bar ----
    let status = format!(
        " {} rows × {} total cols | {} vector column(s) | mode: 1D | ↑↓ scroll rows | ←→ scroll columns | q quit ",
        num_rows, num_cols, total_feat_cols
    );
    let status_widget = Block::default().borders(Borders::ALL).title(status);
    f.render_widget(status_widget, chunks[2]);
}

// ============= helpers (copied / specialized) ===============================

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
            if arr.value(row_idx) {
                "true".to_string()
            } else {
                "false".to_string()
            }
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

fn format_value_12f(array: &ArrayRef, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return "NULL".to_string();
    }
    match array.data_type() {
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            format!("{:.12}", arr.value(row_idx) as f64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{:.12}", arr.value(row_idx))
        }
        _ => format_value(array, row_idx),
    }
}

fn feature_window<'a>(
    all_cols: &'a [usize],
    col_offset: usize,
    visible_cols: usize,
) -> &'a [usize] {
    let start = col_offset.min(all_cols.len());
    let end = (start + visible_cols).min(all_cols.len());
    &all_cols[start..end]
}

fn render_header_1d<'a>(batch: &'a RecordBatch, col_window: &'a [usize]) -> Row<'a> {
    let schema = batch.schema();
    let mut header_cells = vec!["Row".to_string()];
    for &i in col_window {
        header_cells.push(schema.field(i).name().to_string());
    }
    Row::new(header_cells)
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .height(1)
}

fn render_rows_window_1d<'a>(
    batch: &'a RecordBatch,
    col_window: &'a [usize],
    row_start: usize,
    row_end: usize,
) -> Vec<Row<'a>> {
    let mut out = Vec::with_capacity(row_end.saturating_sub(row_start));
    for row_idx in row_start..row_end {
        let mut cells = vec![row_idx.to_string()];
        for &col_idx in col_window {
            let col = batch.column(col_idx);
            let s = format_value_12f(col, row_idx);
            cells.push(s);
        }
        out.push(Row::new(cells).height(1));
    }
    out
}
