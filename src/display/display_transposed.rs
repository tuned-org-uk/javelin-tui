use arrow::array::*;
use arrow::datatypes::DataType;
use arrow_array::RecordBatch;
use ratatui::text::Span;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Modifier, Style},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
};

use crate::display::display::{blend_colors, format_value, get_cell_bg_color};
use crate::display::*;

// === Transposed UI (F×N mode) ==============================================

/// Render transposed header for F×N view (row indices as columns)
fn render_transposed_header<'a>(row_window_start: usize, row_window: &[usize]) -> Row<'a> {
    // Feature index header with special styling
    let mut header_cells = vec![
        Cell::from("Feature").style(
            Style::default()
                .fg(HEADER_FG)
                .bg(HEADER_BG)
                .add_modifier(Modifier::BOLD),
        ),
    ];

    // Row index headers with alternating colors
    for (display_idx, &row_idx) in row_window.iter().enumerate() {
        let col_bg = if (row_window_start + display_idx) % 2 == 0 {
            blend_colors(HEADER_BG, EVEN_COL_BG)
        } else {
            blend_colors(HEADER_BG, ODD_COL_BG)
        };

        header_cells.push(
            Cell::from(format!("R{}", row_idx)).style(
                Style::default()
                    .fg(HEADER_FG)
                    .bg(col_bg)
                    .add_modifier(Modifier::BOLD),
            ),
        );
    }

    // Stats headers with accent color
    header_cells.push(
        Cell::from("avg").style(
            Style::default()
                .fg(TEXT_ACCENT)
                .bg(HEADER_BG)
                .add_modifier(Modifier::BOLD),
        ),
    );
    header_cells.push(
        Cell::from("std").style(
            Style::default()
                .fg(TEXT_ACCENT)
                .bg(HEADER_BG)
                .add_modifier(Modifier::BOLD),
        ),
    );

    Row::new(header_cells).height(1)
}

/// Render transposed rows for F×N view (each row is a feature)
fn render_transposed_rows<'a>(
    batch: &'a RecordBatch,
    all_col_indices: &[usize],
    feat_start: usize,
    feat_end: usize,
    row_window_start: usize,
    row_window: &[usize],
) -> Vec<Row<'a>> {
    let schema = batch.schema();
    let mut out = Vec::with_capacity(feat_end.saturating_sub(feat_start));

    for (feat_display_idx, &feat_idx) in all_col_indices[feat_start..feat_end].iter().enumerate() {
        let feat_abs_idx = feat_start + feat_display_idx;
        let row_bg = if feat_abs_idx % 2 == 0 {
            EVEN_ROW_BG
        } else {
            ODD_ROW_BG
        };

        // Feature name cell
        let mut cells = vec![
            Cell::from(schema.field(feat_idx).name().to_string()).style(
                Style::default()
                    .fg(TEXT_SECONDARY)
                    .bg(row_bg)
                    .add_modifier(Modifier::BOLD),
            ),
        ];

        let col = batch.column(feat_idx);

        // Values for selected rows with alternating column colors
        for (display_idx, &row_idx) in row_window.iter().enumerate() {
            let s = if row_idx < col.len() {
                format_value(col, row_idx)
            } else {
                "OOB".to_string()
            };
            let cell_bg = get_cell_bg_color(feat_abs_idx, row_window_start + display_idx);

            cells.push(Cell::from(s).style(Style::default().fg(TEXT_PRIMARY).bg(cell_bg)));
        }

        // Calculate stats across this feature (all rows)
        let mut vals: Vec<f64> = Vec::new();
        for r in 0..col.len() {
            if col.is_null(r) {
                continue;
            }
            match col.data_type() {
                DataType::Float32 => {
                    let a = col.as_any().downcast_ref::<Float32Array>().unwrap();
                    vals.push(a.value(r) as f64);
                }
                DataType::Float64 => {
                    let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    vals.push(a.value(r));
                }
                DataType::Int32 => {
                    let a = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    vals.push(a.value(r) as f64);
                }
                DataType::Int64 => {
                    let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    vals.push(a.value(r) as f64);
                }
                DataType::UInt32 => {
                    let a = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                    vals.push(a.value(r) as f64);
                }
                DataType::UInt64 => {
                    let a = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                    vals.push(a.value(r) as f64);
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

        // Stats cells with accent color
        cells.push(Cell::from(avg_str).style(Style::default().fg(TEXT_ACCENT).bg(row_bg)));
        cells.push(Cell::from(std_str).style(Style::default().fg(TEXT_ACCENT).bg(row_bg)));

        out.push(Row::new(cells).height(1));
    }

    out
}

/// Render the full UI in transposed mode (F×N: features as rows, samples as columns)
pub(crate) fn render_transposed_ui(
    f: &mut Frame,
    batch: &RecordBatch,
    all_col_indices: &[usize],
    row_offset: usize,
    visible_cols: usize,
    num_rows: usize,
    num_cols: usize,
    feat_start: usize,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // metadata
            Constraint::Min(0),    // table
            Constraint::Length(3), // status
        ])
        .split(f.area());

    let schema = batch.schema();

    // Metadata row with color
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

    let header_paragraph =
        Paragraph::new(Span::styled(meta_text, Style::default().fg(TEXT_SECONDARY))).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BORDER_ACCENT))
                .title(" Metadata "),
        );
    f.render_widget(header_paragraph, chunks[0]);

    // Determine vertical window for features
    let table_area_height = chunks[1].height.saturating_sub(3);
    let max_visible_feats = table_area_height as usize;
    let feat_end = (feat_start + max_visible_feats).min(all_col_indices.len());

    // Horizontal window: which sample rows to show
    let row_window: Vec<usize> = (row_offset..(row_offset + visible_cols).min(num_rows)).collect();

    let header_row = render_transposed_header(row_offset, &row_window);
    let rows = render_transposed_rows(
        batch,
        all_col_indices,
        feat_start,
        feat_end,
        row_offset,
        &row_window,
    );

    let mut widths = vec![Constraint::Length(12)]; // "Feature" column
    for _ in &row_window {
        widths.push(Constraint::Length(12));
    }
    widths.push(Constraint::Length(10)); // avg
    widths.push(Constraint::Length(10)); // std

    let total_feat_cols = all_col_indices.len();
    let start_row = row_offset + 1;
    let end_row = (row_offset + row_window.len()).min(num_rows);

    let title = format!(
        " Lance Data Transposed (features {}–{} of {}, sample rows {}–{} of {}) ",
        feat_start + 1,
        feat_end,
        total_feat_cols,
        start_row,
        end_row,
        num_rows
    );

    let table = Table::new(rows, widths)
        .header(header_row)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BORDER_PRIMARY))
                .title(title),
        )
        .column_spacing(1);

    f.render_widget(table, chunks[1]);

    let status = format!(
        " {} rows × {} total cols | {} feature cols (col_*) | mode: F×N | ↑↓ scroll features | ←→ scroll rows | t transpose | q quit ",
        num_rows, num_cols, total_feat_cols
    );
    let status_widget = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER_ACCENT))
        .title(Span::styled(status, Style::default().fg(TEXT_ACCENT)));
    f.render_widget(status_widget, chunks[2]);
}
