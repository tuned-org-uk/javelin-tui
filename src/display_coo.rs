use anyhow::{Context, Result, anyhow};
use arrow::datatypes::DataType;
use arrow_array::{ArrayRef, Float64Array, RecordBatch, UInt32Array};
use ratatui::text::Span;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph, Row, Table},
};

/// Render one frame for a COO (row, col, value) sparse matrix:
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
/// `triple_offset` controls vertical scrolling in the triples table.
pub fn render_coo_ui(f: &mut Frame, batch: &RecordBatch, triple_offset: usize) {
    // Extract COO components and basic stats.
    let coo = match CooView::from_batch(batch) {
        Ok(c) => c,
        Err(e) => {
            let p = Paragraph::new(Span::raw(format!("Invalid COO dataset: {e}")))
                .block(Block::default().borders(Borders::ALL).title(" Error "));
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

    let meta = Paragraph::new(Span::raw(meta_text)).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" COO Metadata "),
    );
    f.render_widget(meta, outer[0]);

    // --- Middle: left triples table, right sparsity map ----------------------
    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(outer[1]);

    render_triples_table(f, &coo, triple_offset, middle[0]);
    render_sparsity_map(f, &coo, middle[1]);

    // --- Bottom: diagonals + connectivity summary ---------------------------
    let diag_summary = summarize_diagonals(&coo, 6);
    let conn_summary = summarize_connectivity(&coo, 6);

    let summary_text = format!("{diag_summary}\n{conn_summary}");
    let summary = Paragraph::new(summary_text)
        .block(Block::default().borders(Borders::ALL).title(" Structure "));
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

    let header = Row::new(vec!["idx", "row", "col", "value"])
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .height(1);

    let mut rows_ui = Vec::with_capacity(end.saturating_sub(start));
    for i in start..end {
        let r = coo.row.value(i);
        let c = coo.col.value(i);
        let v = coo.val.value(i);
        let cells = vec![
            format!("{i}"),
            format!("{r}"),
            format!("{c}"),
            format!("{:.4}", v),
        ];
        rows_ui.push(Row::new(cells).height(1));
    }

    let widths = vec![
        Constraint::Length(6),
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
        .block(Block::default().borders(Borders::ALL).title(title))
        .column_spacing(1);

    f.render_widget(table, area);
}

// ========================= Sparsity map panel ===============================

fn render_sparsity_map<'a>(f: &mut Frame, coo: &CooView<'a>, area: ratatui::prelude::Rect) {
    let inner_width = area.width.saturating_sub(2) as usize;
    let inner_height = area.height.saturating_sub(2) as usize;
    if inner_width == 0 || inner_height == 0 || coo.n_rows == 0 || coo.n_cols == 0 {
        let p = Paragraph::new("no space / empty matrix")
            .block(Block::default().borders(Borders::ALL).title(" Sparsity "));
        f.render_widget(p, area);
        return;
    }

    // Limit resolution for very large matrices.
    let grid_w = inner_width.min(64);
    let grid_h = inner_height.min(32);

    let mut grid = vec![vec!['.'; grid_w]; grid_h];

    for i in 0..coo.nnz {
        let r = coo.row.value(i) as usize;
        let c = coo.col.value(i) as usize;
        if r >= coo.n_rows || c >= coo.n_cols {
            continue;
        }

        let gr = r * grid_h / coo.n_rows;
        let gc = c * grid_w / coo.n_cols;
        grid[gr][gc] = '*';
    }

    let mut lines = String::new();
    for row in &grid {
        for ch in row {
            lines.push(*ch);
        }
        lines.push('\n');
    }

    let title = format!(
        " Sparsity pattern ({}×{} → {}×{}) ",
        coo.n_rows, coo.n_cols, grid_h, grid_w
    );

    let para = Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(title));
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
