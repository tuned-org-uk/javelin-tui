use arrow::array::*;
use arrow::datatypes::DataType;
use arrow_array::{ArrayRef, RecordBatch};
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Padding;
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
    // 1) First split: metadata / content / status (vertical)
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // metadata
            Constraint::Min(0),    // content area (table + stats panel)
            Constraint::Length(3), // status
        ])
        .split(f.area());

    // 2) Split content area: table on left, stats panel on right (horizontal)
    let content_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(30), // table
            Constraint::Percentage(70), // stats panel
        ])
        .split(main_chunks[1]);

    let schema = batch.schema();

    // ---- Metadata header (unchanged) ----
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
    f.render_widget(header_paragraph, main_chunks[0]);

    // ---- Table rendering (use content_chunks[0] instead of chunks[1]) ----
    let table_area_height = content_chunks[0].height.saturating_sub(3);
    let max_visible_rows = table_area_height as usize;
    let end_row = (row_start + max_visible_rows).min(num_rows);
    let col_window = feature_window(col_indices, col_offset, visible_cols);

    let header_row = render_header_1d(batch, col_window);
    let rows = render_rows_window_1d(batch, col_window, row_start, end_row);

    let mut widths = vec![Constraint::Length(5)];
    for _ in col_window {
        widths.push(Constraint::Length(26));
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
    f.render_widget(table, content_chunks[0]);

    // ---- NEW: Statistics Panel ----
    render_stats_panel(f, batch, col_window, content_chunks[1]);

    // ---- Status bar (unchanged) ----
    let status = format!(
        " {} rows × {} total cols | {} vector column(s) | mode: 1D | ↑↓ scroll rows | ←→ scroll columns | q quit ",
        num_rows, num_cols, total_feat_cols
    );
    let status_widget = Block::default().borders(Borders::ALL).title(status);
    f.render_widget(status_widget, main_chunks[2]);
}

// ============= helpers (copied / specialized) ===============================

fn render_stats_panel(f: &mut Frame, batch: &RecordBatch, col_window: &[usize], area: Rect) {
    if col_window.is_empty() {
        return;
    }

    // Collect all numeric values from visible columns
    let mut all_values: Vec<f64> = Vec::new();
    for &col_idx in col_window {
        let col = batch.column(col_idx);
        for row_idx in 0..batch.num_rows() {
            if !col.is_null(row_idx) {
                if let Some(val) = extract_numeric_value(col, row_idx) {
                    all_values.push(val);
                }
            }
        }
    }

    if all_values.is_empty() {
        let empty_block = Block::default()
            .borders(Borders::ALL)
            .title(" Distribution ");
        f.render_widget(empty_block, area);
        return;
    }

    // Calculate statistics
    all_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean = all_values.iter().sum::<f64>() / all_values.len() as f64;
    let median = if all_values.len() % 2 == 0 {
        (all_values[all_values.len() / 2 - 1] + all_values[all_values.len() / 2]) / 2.0
    } else {
        all_values[all_values.len() / 2]
    };

    // Create histogram
    let min_val = all_values[0];
    let max_val = all_values[all_values.len() - 1];
    let available_width = area.width.saturating_sub(4) as usize;
    let num_bins = 20.min(available_width / 3); // space for columns
    let bin_width = (max_val - min_val) / num_bins as f64;

    let mut bins = vec![0usize; num_bins];
    for &val in &all_values {
        let bin_idx = if bin_width > 0.0 {
            ((val - min_val) / bin_width).floor() as usize
        } else {
            0
        };
        let bin_idx = bin_idx.min(num_bins - 1);
        bins[bin_idx] += 1;
    }

    let max_count = *bins.iter().max().unwrap_or(&1);

    // Build vertical histogram (columns grow upward)
    let chart_height = area.height.saturating_sub(10) as usize; // reserve space for labels
    let mut lines = vec![Line::from("")];

    // Draw histogram rows from top to bottom
    for level in (1..=chart_height).rev() {
        let mut row_str = String::new();
        for &count in &bins {
            let bar_height = if max_count > 0 {
                (count as f64 / max_count as f64 * chart_height as f64).ceil() as usize
            } else {
                0
            };

            if bar_height >= level {
                row_str.push_str("██");
            } else {
                row_str.push_str("  ");
            }
        }
        lines.push(Line::from(row_str));
    }

    // Add baseline
    lines.push(Line::from("─".repeat(num_bins * 2)));

    // Add value range axis
    let axis_line = format!(
        "{:<8.2}{}>{:>8.2}",
        min_val,
        " ".repeat(num_bins * 2 - 18),
        max_val
    );
    lines.push(Line::from(axis_line));
    lines.push(Line::from(""));

    // Statistics on one line horizontally
    let stats_line = format!(
        "Count: {}  │  Mean: {:.6}  │  Median: {:.6}",
        all_values.len(),
        mean,
        median
    );
    lines.push(Line::from(stats_line).style(Style::default().fg(Color::Cyan)));

    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Distribution ")
            .padding(Padding::horizontal(2)), // 2 spaces on left and right
    );
    f.render_widget(paragraph, area);
}

fn extract_numeric_value(array: &ArrayRef, row_idx: usize) -> Option<f64> {
    match array.data_type() {
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Some(arr.value(row_idx) as f64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Some(arr.value(row_idx))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Some(arr.value(row_idx) as f64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Some(arr.value(row_idx) as f64)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Some(arr.value(row_idx) as f64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Some(arr.value(row_idx) as f64)
        }
        _ => None,
    }
}

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
