//! Interactive TUI visualization for sparse matrix connectivity graphs
//!
//! Displays the connectivity graph with interactive navigation and multiple views.

use crate::functions::sparse_viz::ConnectivityGraph;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};
use std::io;

// Color palette
use crate::display::*;

const TEXT_WARNING: Color = Color::Rgb(255, 121, 198);

/// View mode for the connectivity visualization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewMode {
    Overview,   // Summary statistics and top hubs
    Nodes,      // Detailed node list with scrolling
    Edges,      // Edge list with weights
    Components, // Connected components view
}

impl ViewMode {
    fn next(&self) -> Self {
        match self {
            ViewMode::Overview => ViewMode::Nodes,
            ViewMode::Nodes => ViewMode::Edges,
            ViewMode::Edges => ViewMode::Components,
            ViewMode::Components => ViewMode::Overview,
        }
    }

    fn prev(&self) -> Self {
        match self {
            ViewMode::Overview => ViewMode::Components,
            ViewMode::Nodes => ViewMode::Overview,
            ViewMode::Edges => ViewMode::Nodes,
            ViewMode::Components => ViewMode::Edges,
        }
    }

    fn as_str(&self) -> &str {
        match self {
            ViewMode::Overview => "Overview",
            ViewMode::Nodes => "Nodes",
            ViewMode::Edges => "Edges",
            ViewMode::Components => "Components",
        }
    }
}

/// Interactive viewer for sparse matrix connectivity
pub fn display_connectivity_interactive(batch: &RecordBatch) -> Result<()> {
    // Build connectivity graph
    let graph = ConnectivityGraph::from_coo_batch(batch)?;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut view_mode = ViewMode::Overview;
    let mut scroll_offset = 0;
    let mut selected_node: Option<usize> = None;

    loop {
        terminal
            .draw(|f| render_connectivity_ui(f, &graph, view_mode, scroll_offset, selected_node))?;

        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(KeyEvent { code, .. }) = event::read()? {
                match code {
                    KeyCode::Char('q') | KeyCode::Esc => break,

                    // View mode switching
                    KeyCode::Tab => {
                        view_mode = view_mode.next();
                        scroll_offset = 0;
                    }
                    KeyCode::BackTab => {
                        view_mode = view_mode.prev();
                        scroll_offset = 0;
                    }

                    // Vertical scrolling
                    KeyCode::Up | KeyCode::Char('k') => {
                        if scroll_offset > 0 {
                            scroll_offset -= 1;
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        scroll_offset += 1;
                    }
                    KeyCode::PageUp => {
                        scroll_offset = scroll_offset.saturating_sub(10);
                    }
                    KeyCode::PageDown => {
                        scroll_offset += 10;
                    }
                    KeyCode::Home | KeyCode::Char('g') => {
                        scroll_offset = 0;
                    }
                    KeyCode::End | KeyCode::Char('G') => {
                        scroll_offset = usize::MAX; // Will be clamped
                    }

                    // Node selection (when in Nodes view)
                    KeyCode::Enter => {
                        if view_mode == ViewMode::Nodes && scroll_offset < graph.nodes.len() {
                            selected_node = Some(scroll_offset);
                        }
                    }
                    KeyCode::Char('c') => {
                        selected_node = None;
                    }

                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

/// Main rendering function
fn render_connectivity_ui(
    f: &mut Frame,
    graph: &ConnectivityGraph,
    view_mode: ViewMode,
    scroll_offset: usize,
    selected_node: Option<usize>,
) {
    // Main layout: header, content, footer
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Content
            Constraint::Length(3), // Footer
        ])
        .split(f.area());

    render_header(f, graph, chunks[0]);

    match view_mode {
        ViewMode::Overview => render_overview(f, graph, chunks[1]),
        ViewMode::Nodes => render_nodes_view(f, graph, chunks[1], scroll_offset, selected_node),
        ViewMode::Edges => render_edges_view(f, graph, chunks[1], scroll_offset),
        ViewMode::Components => render_components_view(f, graph, chunks[1], scroll_offset),
    }

    render_footer(f, view_mode, chunks[2]);
}

/// Render header with graph statistics
fn render_header(f: &mut Frame, graph: &ConnectivityGraph, area: Rect) {
    let total_degree: usize = graph.nodes.iter().map(|n| n.degree).sum();
    let avg_degree = if !graph.nodes.is_empty() {
        total_degree as f64 / graph.nodes.len() as f64
    } else {
        0.0
    };

    let isolated = graph.get_isolated_nodes().len();
    let components = graph.connected_components().len();

    let header_text = format!(
        "Graph: {} nodes, {} edges | Avg degree: {:.2} | Isolated: {} | Components: {}",
        graph.nodes.len(),
        graph.edges.len(),
        avg_degree,
        isolated,
        components
    );

    let header = Paragraph::new(Span::styled(
        header_text,
        Style::default().fg(TEXT_SECONDARY),
    ))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_ACCENT))
            .title(" Connectivity Graph "),
    );

    f.render_widget(header, area);
}

/// Render footer with controls
fn render_footer(f: &mut Frame, view_mode: ViewMode, area: Rect) {
    let controls = format!(
        "View: {} | Tab/Shift+Tab: Switch view | ↑↓/jk: Scroll | Enter: Select | c: Clear | q: Quit",
        view_mode.as_str()
    );

    let footer = Paragraph::new(Span::styled(controls, Style::default().fg(TEXT_ACCENT))).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_PRIMARY)),
    );

    f.render_widget(footer, area);
}

/// Render overview with statistics and top hubs
fn render_overview(f: &mut Frame, graph: &ConnectivityGraph, area: Rect) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Left: Statistics
    render_statistics(f, graph, layout[0]);

    // Right: Top hubs
    render_top_hubs(f, graph, layout[1]);
}

fn render_statistics(f: &mut Frame, graph: &ConnectivityGraph, area: Rect) {
    let components = graph.connected_components();
    let largest_component = components.iter().map(|c| c.len()).max().unwrap_or(0);

    let total_weight: f64 = graph.edges.iter().map(|e| e.weight).sum();
    let avg_weight = if !graph.edges.is_empty() {
        total_weight / graph.edges.len() as f64
    } else {
        0.0
    };

    let max_degree = graph.nodes.iter().map(|n| n.degree).max().unwrap_or(0);

    let lines = vec![
        Line::from(Span::styled(
            "Graph Statistics",
            Style::default().fg(HEADER_FG).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("Matrix: ", Style::default().fg(TEXT_SECONDARY)),
            Span::styled(
                format!("{}×{}", graph.n_rows, graph.n_cols),
                Style::default().fg(TEXT_PRIMARY),
            ),
        ]),
        Line::from(vec![
            Span::styled("Total nodes: ", Style::default().fg(TEXT_SECONDARY)),
            Span::styled(
                format!("{}", graph.nodes.len()),
                Style::default().fg(TEXT_PRIMARY),
            ),
        ]),
        Line::from(vec![
            Span::styled("Total edges: ", Style::default().fg(TEXT_SECONDARY)),
            Span::styled(
                format!("{}", graph.edges.len()),
                Style::default().fg(TEXT_PRIMARY),
            ),
        ]),
        Line::from(vec![
            Span::styled("Max degree: ", Style::default().fg(TEXT_SECONDARY)),
            Span::styled(format!("{}", max_degree), Style::default().fg(TEXT_WARNING)),
        ]),
        Line::from(vec![
            Span::styled("Avg edge weight: ", Style::default().fg(TEXT_SECONDARY)),
            Span::styled(
                format!("{:.2}", avg_weight),
                Style::default().fg(TEXT_PRIMARY),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Components: ", Style::default().fg(TEXT_SECONDARY)),
            Span::styled(
                format!("{}", components.len()),
                Style::default().fg(TEXT_ACCENT),
            ),
        ]),
        Line::from(vec![
            Span::styled("Largest component: ", Style::default().fg(TEXT_SECONDARY)),
            Span::styled(
                format!("{} nodes", largest_component),
                Style::default().fg(TEXT_PRIMARY),
            ),
        ]),
    ];

    let para = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_PRIMARY))
            .title(" Statistics "),
    );

    f.render_widget(para, area);
}

fn render_top_hubs(f: &mut Frame, graph: &ConnectivityGraph, area: Rect) {
    let hubs = graph.get_hubs(20);

    let items: Vec<ListItem> = hubs
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let bg = if i % 2 == 0 { EVEN_ROW_BG } else { ODD_ROW_BG };
            let connections_preview: Vec<String> = node
                .connected_to
                .iter()
                .take(5)
                .map(|n| n.to_string())
                .collect();

            let line = Line::from(vec![
                Span::styled(
                    format!("{:3}. ", i + 1),
                    Style::default().fg(TEXT_SECONDARY).bg(bg),
                ),
                Span::styled(
                    format!("Node {:3} ", node.id),
                    Style::default()
                        .fg(TEXT_ACCENT)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("(deg={:3}) → ", node.degree),
                    Style::default().fg(TEXT_WARNING).bg(bg),
                ),
                Span::styled(
                    format!(
                        "[{}{}]",
                        connections_preview.join(", "),
                        if node.connected_to.len() > 5 {
                            "..."
                        } else {
                            ""
                        }
                    ),
                    Style::default().fg(TEXT_PRIMARY).bg(bg),
                ),
            ]);

            ListItem::new(line)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_PRIMARY))
            .title(" Top Connected Nodes (Hubs) "),
    );

    f.render_widget(list, area);
}

/// Render detailed nodes view with scrolling
fn render_nodes_view(
    f: &mut Frame,
    graph: &ConnectivityGraph,
    area: Rect,
    scroll_offset: usize,
    selected_node: Option<usize>,
) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    // Left: Node list
    let inner_height = layout[0].height.saturating_sub(2) as usize;
    let start = scroll_offset.min(graph.nodes.len().saturating_sub(1));
    let end = (start + inner_height).min(graph.nodes.len());

    let items: Vec<ListItem> = graph.nodes[start..end]
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let idx = start + i;
            let bg = if idx % 2 == 0 {
                EVEN_ROW_BG
            } else {
                ODD_ROW_BG
            };
            let is_selected = selected_node == Some(idx);

            let style = if is_selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(TEXT_ACCENT)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().bg(bg)
            };

            let line = Line::from(vec![
                Span::styled(format!("{:4} ", node.id), style.fg(TEXT_ACCENT)),
                Span::styled(
                    format!("deg={:3} ", node.degree),
                    style.fg(if node.degree > 10 {
                        TEXT_WARNING
                    } else {
                        TEXT_PRIMARY
                    }),
                ),
                Span::styled(
                    if node.degree == 0 {
                        "(isolated)".to_string()
                    } else {
                        format!("→ {} nodes", node.connected_to.len())
                    },
                    style.fg(TEXT_SECONDARY),
                ),
            ]);

            ListItem::new(line)
        })
        .collect();

    let title = format!(
        " Nodes [{}-{} of {}] ",
        start,
        end.saturating_sub(1),
        graph.nodes.len()
    );
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_PRIMARY))
            .title(title),
    );

    f.render_widget(list, layout[0]);

    // Right: Selected node details
    render_node_details(f, graph, layout[1], selected_node);
}

fn render_node_details(
    f: &mut Frame,
    graph: &ConnectivityGraph,
    area: Rect,
    selected_node: Option<usize>,
) {
    let text = if let Some(node_id) = selected_node {
        if node_id < graph.nodes.len() {
            let node = &graph.nodes[node_id];
            let mut lines = vec![
                Line::from(vec![
                    Span::styled("Node ID: ", Style::default().fg(TEXT_SECONDARY)),
                    Span::styled(
                        format!("{}", node.id),
                        Style::default()
                            .fg(TEXT_ACCENT)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("Degree: ", Style::default().fg(TEXT_SECONDARY)),
                    Span::styled(
                        format!("{}", node.degree),
                        Style::default().fg(TEXT_WARNING),
                    ),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "Connected to:",
                    Style::default()
                        .fg(TEXT_SECONDARY)
                        .add_modifier(Modifier::UNDERLINED),
                )),
            ];

            for (i, &neighbor) in node.connected_to.iter().enumerate() {
                if i >= 15 {
                    lines.push(Line::from(Span::styled(
                        format!("  ... and {} more", node.connected_to.len() - i),
                        Style::default().fg(TEXT_SECONDARY),
                    )));
                    break;
                }
                lines.push(Line::from(Span::styled(
                    format!("  • Node {}", neighbor),
                    Style::default().fg(TEXT_PRIMARY),
                )));
            }

            lines
        } else {
            vec![Line::from(Span::styled(
                "Invalid node selection",
                Style::default().fg(Color::Red),
            ))]
        }
    } else {
        vec![
            Line::from(Span::styled(
                "No node selected",
                Style::default().fg(TEXT_SECONDARY),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Press Enter to select",
                Style::default().fg(TEXT_ACCENT),
            )),
        ]
    };

    let para = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_ACCENT))
            .title(" Node Details "),
    );

    f.render_widget(para, area);
}

/// Render edges view
fn render_edges_view(f: &mut Frame, graph: &ConnectivityGraph, area: Rect, scroll_offset: usize) {
    let inner_height = area.height.saturating_sub(2) as usize;
    let start = scroll_offset.min(graph.edges.len().saturating_sub(1));
    let end = (start + inner_height).min(graph.edges.len());

    let items: Vec<ListItem> = graph.edges[start..end]
        .iter()
        .enumerate()
        .map(|(i, edge)| {
            let idx = start + i;
            let bg = if idx % 2 == 0 {
                EVEN_ROW_BG
            } else {
                ODD_ROW_BG
            };

            let line = Line::from(vec![
                Span::styled(
                    format!("{:4}. ", idx),
                    Style::default().fg(TEXT_SECONDARY).bg(bg),
                ),
                Span::styled(
                    format!("{:3} ", edge.from),
                    Style::default().fg(TEXT_ACCENT).bg(bg),
                ),
                Span::styled("⟷ ", Style::default().fg(TEXT_PRIMARY).bg(bg)),
                Span::styled(
                    format!("{:3} ", edge.to),
                    Style::default().fg(TEXT_ACCENT).bg(bg),
                ),
                Span::styled(
                    format!("(weight={:.1})", edge.weight),
                    Style::default().fg(TEXT_WARNING).bg(bg),
                ),
            ]);

            ListItem::new(line)
        })
        .collect();

    let title = format!(
        " Edges [{}-{} of {}] ",
        start,
        end.saturating_sub(1),
        graph.edges.len()
    );
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_PRIMARY))
            .title(title),
    );

    f.render_widget(list, area);
}

/// Render connected components view
fn render_components_view(
    f: &mut Frame,
    graph: &ConnectivityGraph,
    area: Rect,
    scroll_offset: usize,
) {
    let components = graph.connected_components();
    let inner_height = area.height.saturating_sub(2) as usize;
    let start = scroll_offset.min(components.len().saturating_sub(1));
    let end = (start + inner_height).min(components.len());

    let items: Vec<ListItem> = components[start..end]
        .iter()
        .enumerate()
        .map(|(i, comp)| {
            let idx = start + i;
            let bg = if idx % 2 == 0 {
                EVEN_ROW_BG
            } else {
                ODD_ROW_BG
            };

            let preview: Vec<String> = comp.iter().take(10).map(|n| n.to_string()).collect();
            let preview_str = if comp.len() > 10 {
                format!("[{}...]", preview.join(", "))
            } else {
                format!("[{}]", preview.join(", "))
            };

            let line = Line::from(vec![
                Span::styled(
                    format!("Component {:3}: ", idx),
                    Style::default()
                        .fg(TEXT_ACCENT)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("{:4} nodes ", comp.len()),
                    Style::default().fg(TEXT_WARNING).bg(bg),
                ),
                Span::styled(preview_str, Style::default().fg(TEXT_PRIMARY).bg(bg)),
            ]);

            ListItem::new(line)
        })
        .collect();

    let title = format!(
        " Connected Components [{}-{} of {}] ",
        start,
        end.saturating_sub(1),
        components.len()
    );
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER_PRIMARY))
            .title(title),
    );

    f.render_widget(list, area);
}
