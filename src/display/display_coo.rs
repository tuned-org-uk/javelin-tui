use anyhow::{Context, Result, anyhow};
use arrow_array::{Float64Array, RecordBatch, UInt32Array};
use ratatui::text::Span;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
};

use crate::display::*;

/// Render one frame for a COO (row, col, value) sparse matrix.
///
/// Layout:
///   ┌───────────────────────────────────────────────┐
///   │ Metadata / summary                            │
///   ├───────────────────────────────────────────────┤
///   │ Triples table (left)  |  Sparsity ASCII map  │
///   ├───────────────────────────────────────────────┤
///   │ Diagonals / connectivity summary              │
///   └───────────────────────────────────────────────┘
///
/// `triple_offset` controls vertical scrolling in the triples table
/// and the visible row band in the sparsity map.
pub(crate) fn render_coo_ui(
    f: &mut Frame,
    batch: &RecordBatch,
    triple_offset: usize,
    col_offset: usize,
) {
    // Extract COO components and basic stats.
    let coo = match CooView::from_batch(batch) {
        Ok(c) => c,
        Err(e) => {
            let p = Paragraph::new(Span::styled(
                format!("Invalid COO dataset: {e}"),
                Style::default().fg(Color::Red),
            ))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Red))
                    .title(" Error "),
            );
            f.render_widget(p, f.area());
            return;
        }
    };

    let nnz = coo.nnz;
    let (n_rows, n_cols) = (coo.n_rows, coo.n_cols);

    // Top (metadata), middle (triples + sparsity), bottom (diagonals/connectivity).
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(4),
        ])
        .split(f.area());

    // --- Top: metadata line ---------------------------------------------------
    let meta_text = format!(
        "rows: {}  cols: {}  nnz: {}  density: {:.6}",
        n_rows,
        n_cols,
        nnz,
        if n_rows == 0 || n_cols == 0 {
            0.0
        } else {
            (nnz as f64) / ((n_rows * n_cols) as f64)
        }
    );

    let meta = Paragraph::new(Span::styled(meta_text, Style::default().fg(TEXT_SECONDARY))).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_ACCENT))
            .title(" Sparse Representation "),
    );
    f.render_widget(meta, outer[0]);

    // --- Middle: left triples table, right sparsity map ----------------------
    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(outer[1]);

    render_triples_table(f, &coo, triple_offset, middle[0]);
    render_sparsity_map(f, &coo, middle[1], triple_offset, col_offset);

    // --- Bottom: diagonals + connectivity summary ---------------------------
    let diag_summary = summarize_diagonals(&coo, 6);
    let conn_summary = summarize_connectivity(&coo, 6);

    let summary_text = format!("{diag_summary}\n{conn_summary}");
    let summary = Paragraph::new(Span::styled(summary_text, Style::default().fg(TEXT_ACCENT)))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BORDER_PRIMARY))
                .title(" Structure "),
        );
    f.render_widget(summary, outer[2]);
}

// ======================= Internal COO helpers ===============================

struct CooView<'a> {
    row: &'a UInt32Array,
    col: &'a UInt32Array,
    val: &'a Float64Array,
    n_rows: usize,
    n_cols: usize,
    nnz: usize,
}

impl<'a> CooView<'a> {
    fn from_batch(batch: &'a RecordBatch) -> Result<Self> {
        if batch.num_columns() < 3 {
            return Err(anyhow!(
                "expected at least 3 columns (row, col, value), got {}",
                batch.num_columns()
            ));
        }

        // Locate row/col/value by name, regardless of order.
        let schema = batch.schema();
        let mut row_idx = None;
        let mut col_idx = None;
        let mut val_idx = None;

        for (i, f) in schema.fields().iter().enumerate() {
            match f.name().as_str() {
                "row" => row_idx = Some(i),
                "col" => col_idx = Some(i),
                "value" => val_idx = Some(i),
                _ => {}
            }
        }

        let (row_i, col_i, val_i) = match (row_idx, col_idx, val_idx) {
            (Some(r), Some(c), Some(v)) => (r, c, v),
            _ => {
                return Err(anyhow!(
                    "COO schema must contain columns named 'row', 'col', and 'value'"
                ));
            }
        };

        let row = batch
            .column(row_i)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .context("row must be UInt32")?;
        let col = batch
            .column(col_i)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .context("col must be UInt32")?;
        let val = batch
            .column(val_i)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("value must be Float64")?;

        let nnz = row.len();
        if col.len() != nnz || val.len() != nnz {
            return Err(anyhow!(
                "row/col/value length mismatch: row={}, col={}, value={}",
                nnz,
                col.len(),
                val.len()
            ));
        }

        // Determine matrix dimensions from schema metadata or infer from max index.
        let md = schema.metadata();
        let mut n_rows = None;
        let mut n_cols = None;

        if let Some(r) = md.get("rows") {
            n_rows = r.parse::<usize>().ok();
        }
        if let Some(c) = md.get("cols") {
            n_cols = c.parse::<usize>().ok();
        }

        let (nr, nc) = match (n_rows, n_cols) {
            (Some(r), Some(c)) => (r, c),
            _ => {
                // Fallback: infer as 1 + max(row), 1 + max(col)
                let mut max_r = 0u32;
                let mut max_c = 0u32;
                for i in 0..nnz {
                    let rv = row.value(i);
                    let cv = col.value(i);
                    if rv > max_r {
                        max_r = rv;
                    }
                    if cv > max_c {
                        max_c = cv;
                    }
                }
                (max_r as usize + 1, max_c as usize + 1)
            }
        };

        Ok(Self {
            row,
            col,
            val,
            n_rows: nr,
            n_cols: nc,
            nnz,
        })
    }
}

// ========================= Triples table panel ==============================

fn render_triples_table<'a>(
    f: &mut Frame,
    coo: &CooView<'a>,
    triple_offset: usize,
    area: ratatui::prelude::Rect,
) {
    // Leave room for header row inside the bordered block.
    let inner_height = area.height.saturating_sub(2); // borders
    if inner_height <= 1 {
        let p = Paragraph::new("area too small")
            .block(Block::default().borders(Borders::ALL).title(" Triples "));
        f.render_widget(p, area);
        return;
    }

    let max_visible = (inner_height - 1) as usize; // minus header
    let start = triple_offset.min(coo.nnz);
    let end = (start + max_visible).min(coo.nnz);

    // Header with color
    let header = Row::new(vec![
        // Cell::from("idx").style(
        //     Style::default()
        //         .fg(HEADER_FG)
        //         .bg(HEADER_BG)
        //         .add_modifier(Modifier::BOLD),
        // ),
        Cell::from("row").style(
            Style::default()
                .fg(HEADER_FG)
                .bg(HEADER_BG)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("col").style(
            Style::default()
                .fg(HEADER_FG)
                .bg(HEADER_BG)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from("value").style(
            Style::default()
                .fg(HEADER_FG)
                .bg(HEADER_BG)
                .add_modifier(Modifier::BOLD),
        ),
    ])
    .height(1);

    // Rows with alternating colors
    let mut rows_ui = Vec::with_capacity(end.saturating_sub(start));
    for i in start..end {
        let r = coo.row.value(i);
        let c = coo.col.value(i);
        let v = coo.val.value(i);

        let row_bg = if (i - start) % 2 == 0 {
            EVEN_ROW_BG
        } else {
            ODD_ROW_BG
        };

        let cells = vec![
            // Cell::from(format!("{i}")).style(
            //     Style::default()
            //         .fg(TEXT_SECONDARY)
            //         .bg(row_bg)
            //         .add_modifier(Modifier::BOLD),
            // ),
            Cell::from(format!("{r}")).style(Style::default().fg(TEXT_PRIMARY).bg(row_bg)),
            Cell::from(format!("{c}")).style(Style::default().fg(TEXT_PRIMARY).bg(row_bg)),
            Cell::from(format!("{:.4}", v)).style(Style::default().fg(TEXT_PRIMARY).bg(row_bg)),
        ];
        rows_ui.push(Row::new(cells).height(1));
    }

    let widths = vec![
        // Constraint::Length(6),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(14),
    ];

    let title = format!(
        " Triples [{}–{} of {}] ",
        if coo.nnz == 0 { 0 } else { start },
        end,
        coo.nnz
    );

    let table = Table::new(rows_ui, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BORDER_PRIMARY))
                .title(title),
        )
        .column_spacing(1);

    f.render_widget(table, area);
}

// ========================= Sparsity map panel ===============================

fn render_sparsity_map<'a>(
    f: &mut Frame,
    coo: &CooView<'a>,
    area: ratatui::prelude::Rect,
    triple_offset: usize,
    col_offset: usize,
) {
    let inner_width = area.width.saturating_sub(2) as usize;
    let inner_height = area.height.saturating_sub(2) as usize;
    if inner_width == 0 || inner_height == 0 || coo.n_rows == 0 || coo.n_cols == 0 {
        let p = Paragraph::new("no space / empty matrix")
            .block(Block::default().borders(Borders::ALL).title(" Sparsity "));
        f.render_widget(p, area);
        return;
    }

    // Determine visible row window
    let row_start = triple_offset.min(coo.n_rows.saturating_sub(1));
    let row_end = (row_start + inner_height).min(coo.n_rows);
    let visible_rows = row_end - row_start;

    // Determine visible column window with horizontal scrolling
    let col_start = col_offset.min(coo.n_cols.saturating_sub(1));
    let col_end = (col_start + inner_width).min(coo.n_cols);
    let visible_cols = col_end - col_start;

    // Create grid with 1:1 mapping (no downsampling)
    let mut grid = vec![vec![false; visible_cols]; visible_rows];

    // Map each non-zero entry to the grid with 1:1 mapping
    for i in 0..coo.nnz {
        let r = coo.row.value(i) as usize;
        let c = coo.col.value(i) as usize;

        // Check if this entry is in the visible window
        if r >= row_start && r < row_end && c >= col_start && c < col_end {
            let gr = r - row_start;
            let gc = c - col_start;
            if gr < visible_rows && gc < visible_cols {
                grid[gr][gc] = true;
            }
        }
    }

    // Build colored text with asterisks and dots
    let mut lines = Vec::new();
    for row in &grid {
        let mut spans = Vec::new();
        for &has_value in row {
            if has_value {
                spans.push(Span::styled(
                    "*",
                    Style::default()
                        .fg(SPARSE_ASTERISK)
                        .add_modifier(Modifier::BOLD),
                ));
            } else {
                spans.push(Span::styled("·", Style::default().fg(SPARSE_DOT)));
            }
        }
        lines.push(Line::from(spans));
    }

    let title = format!(
        " Sparsity rows {}–{} of {}, cols {}–{} of {} (←→ to scroll cols) ",
        row_start,
        row_end.saturating_sub(1),
        coo.n_rows,
        col_start,
        col_end.saturating_sub(1),
        coo.n_cols
    );

    let para = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_ACCENT))
            .title(title),
    );
    f.render_widget(para, area);
}

// ===================== Diagonals / connectivity summary =====================

fn summarize_diagonals(coo: &CooView<'_>, max_items: usize) -> String {
    let mut entries = Vec::new();
    for i in 0..coo.nnz {
        let r = coo.row.value(i) as usize;
        let c = coo.col.value(i) as usize;
        if r == c {
            entries.push((r, coo.val.value(i)));
        }
    }

    if entries.is_empty() {
        return "Diagonals: no non-zero entries on main diagonal".to_string();
    }

    entries.sort_by_key(|(r, _)| *r);
    entries.truncate(max_items);

    let mut s = String::from("Diagonals (row == col):");
    for (r, v) in entries {
        s.push_str(&format!("  ({r}, {r}): {:.4}", v));
    }
    s
}

fn summarize_connectivity(coo: &CooView<'_>, max_rows: usize) -> String {
    if coo.n_rows == 0 {
        return "Connectivity: empty matrix".to_string();
    }

    let mut counts = vec![0usize; coo.n_rows];
    for i in 0..coo.nnz {
        let r = coo.row.value(i) as usize;
        if r < coo.n_rows {
            counts[r] += 1;
        }
    }

    // Collect (row, count) and sort by descending count.
    let mut rows: Vec<(usize, usize)> = counts
        .iter()
        .enumerate()
        .filter(|(_, c)| **c > 0)
        .map(|(r, &c)| (r, c))
        .collect();

    if rows.is_empty() {
        return "Connectivity: all rows are zero".to_string();
    }

    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows.truncate(max_rows);

    let mut s = String::from("Most connected rows (by nnz):");
    for (r, c) in rows {
        s.push_str(&format!("  row {r}: {c} connections"));
    }
    s
}
