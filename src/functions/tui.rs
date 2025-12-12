use anyhow::Result;
use std::path::PathBuf;

use crate::functions::{display::cmd_display, head::cmd_head, sample::cmd_sample};

pub async fn run_tui(root: PathBuf) -> Result<()> {
    use crossterm::{
        ExecutableCommand,
        event::{self, Event, KeyCode},
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    };
    use ratatui::{
        Terminal,
        backend::CrosstermBackend,
        layout::{Constraint, Direction, Layout},
        style::{Color, Modifier, Style},
        widgets::{Block, Borders, List, ListItem, Paragraph},
    };
    use std::fs;
    use std::io::stdout;

    // 1. Check directory and collect .lance children
    if !root.is_dir() {
        return Err(anyhow::anyhow!(format!(
            "Path is not a directory: {:?}",
            root
        )));
    }

    let mut entries: Vec<PathBuf> = fs::read_dir(&root)
        .map_err(|e| anyhow::anyhow!(format!("Failed to read dir {:?}: {}", root, e)))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("lance"))
                .unwrap_or(false)
        })
        .collect();

    entries.sort();

    if entries.is_empty() {
        return Err(anyhow::anyhow!(format!(
            "No .lance files found in directory {:?}. Tui command works with directories, \
            for files use display command",
            root
        )));
    }

    // Commands we support from TUI
    #[derive(Clone, Copy)]
    enum TuiCommand {
        Head,
        Sample,
        Display,
    }
    let commands = [TuiCommand::Head, TuiCommand::Sample, TuiCommand::Display];

    let mut selected_file_idx: usize = 0;
    let mut selected_cmd_idx: usize = 0;

    // 2. Setup terminal
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    loop {
        terminal.draw(|frame| {
            let size = frame.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3), // title
                    Constraint::Min(5),    // file list
                    Constraint::Length(5), // command selector
                ])
                .split(size);

            // Header
            let header = Paragraph::new(format!(
                "Javelin - Lance Inspector\nDirectory: {}",
                root.display()
            ))
            .block(Block::default().borders(Borders::ALL).title(" Info "));
            frame.render_widget(header, chunks[0]);

            // File list
            let items: Vec<ListItem> = entries
                .iter()
                .map(|p| {
                    let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("<?>");
                    ListItem::new(name.to_string())
                })
                .collect();

            let file_list = List::new(items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Lance files "),
                )
                .highlight_style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
                .highlight_symbol(">> ");

            frame.render_stateful_widget(
                file_list,
                chunks[1],
                &mut ratatui::widgets::ListState::default().with_selected(Some(selected_file_idx)),
            );

            // Command chooser
            let cmd_labels: Vec<&str> = commands
                .iter()
                .map(|c| match c {
                    TuiCommand::Head => "Head",
                    TuiCommand::Sample => "Sample",
                    TuiCommand::Display => "Display",
                })
                .collect();

            let mut cmd_spans = String::new();
            for (i, label) in cmd_labels.iter().enumerate() {
                if i == selected_cmd_idx {
                    cmd_spans.push_str(&format!("[{}]  ", label));
                } else {
                    cmd_spans.push_str(&format!(" {}   ", label));
                }
            }

            let cmd_para = Paragraph::new(cmd_spans)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Command (←/→ to change, Enter to run, q to quit) "),
                )
                .style(Style::default().fg(Color::White));

            frame.render_widget(cmd_para, chunks[2]);
        })?;

        // Handle input
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        break;
                    }
                    // File selection up/down
                    KeyCode::Up | KeyCode::Char('k') => {
                        if selected_file_idx > 0 {
                            selected_file_idx -= 1;
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if selected_file_idx + 1 < entries.len() {
                            selected_file_idx += 1;
                        }
                    }
                    // Command selection left/right
                    KeyCode::Left | KeyCode::Char('h') => {
                        if selected_cmd_idx > 0 {
                            selected_cmd_idx -= 1;
                        }
                    }
                    KeyCode::Right | KeyCode::Char('l') => {
                        if selected_cmd_idx + 1 < commands.len() {
                            selected_cmd_idx += 1;
                        }
                    }
                    // Enter: run selected command on selected file
                    KeyCode::Enter => {
                        let file = entries[selected_file_idx].clone();
                        let cmd = commands[selected_cmd_idx];

                        // Leave current TUI before launching nested viewer
                        disable_raw_mode()?;
                        terminal.backend_mut().execute(LeaveAlternateScreen)?;
                        terminal.show_cursor()?;

                        // Reuse existing async command functions
                        match cmd {
                            TuiCommand::Head => {
                                // default n=20 for example; you can tune or prompt later
                                cmd_head(&file, 20).await?;
                            }
                            TuiCommand::Sample => {
                                cmd_sample(&file, 20).await?;
                            }
                            TuiCommand::Display => {
                                cmd_display(&file).await?;
                            }
                        }

                        // Re-enter launcher TUI after the viewer exits
                        enable_raw_mode()?;
                        stdout().execute(EnterAlternateScreen)?;
                        let backend = CrosstermBackend::new(stdout());
                        terminal = Terminal::new(backend)?;
                    }
                    _ => {}
                }
            }
        }
    }

    // Cleanup
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
