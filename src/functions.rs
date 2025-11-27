use anyhow::{Context, Result};
use lance::dataset::Dataset;
use std::path::PathBuf;

use crate::display::*;

pub async fn cmd_info(filepath: &PathBuf) -> Result<()> {
    println!("=== Lance File Info ===");
    println!("Path: {}", filepath.display());

    // Open the Lance dataset
    let uri = format!("file://{}", filepath.canonicalize()?.display());
    let dataset = lance::dataset::Dataset::open(&uri)
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
        println!("  - {} : {:?}", idx, f);
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
                "  {} - {} bytes",
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

    display_spreadsheet_interactive(&batch)?;

    Ok(())
}

pub async fn cmd_sample(filepath: &PathBuf, n: usize) -> Result<()> {
    use lance::dataset::Dataset;
    use rand::seq::SliceRandom;

    println!("=== {} random samples ===", n);

    let uri = format!("file://{}", filepath.canonicalize()?.display());
    let dataset = Dataset::open(&uri).await?;

    let total_rows = dataset.count_rows(None).await?;

    if total_rows == 0 {
        println!("Dataset is empty");
        return Ok(());
    }

    // Generate random indices
    let mut rng = rand::rng();
    let mut indices: Vec<usize> = (0..total_rows).collect();
    indices.shuffle(&mut rng);
    indices.truncate(n.min(total_rows));
    indices.sort_unstable();

    println!("Sampling {} rows from {} total", indices.len(), total_rows);
    println!("Indices: {:?}", indices);

    // Note: Lance doesn't have direct row indexing, so we'd need to scan
    // For now, just show the approach
    println!("\n(Full sampling implementation requires scanning and filtering)");

    Ok(())
}

pub async fn cmd_stats(filepath: &PathBuf) -> Result<()> {
    use lance::dataset::Dataset;

    println!("=== Dataset Statistics ===");

    let uri = format!("file://{}", filepath.canonicalize()?.display());
    let dataset = Dataset::open(&uri).await?;

    let schema = dataset.schema();
    let count = dataset.count_rows(None).await?;

    println!("Total rows: {}", count);
    println!("Schema: {}", schema.to_string());

    // Compute basic stats per column
    println!("\nColumn statistics:");
    for idx in schema.field_ids() {
        let f = schema.field_by_id(idx).unwrap();
        println!("  {}:", f.to_string());
        println!("    Type: {:?}", f.data_type());
        println!("  - {} : {:?}", idx, f);
    }

    Ok(())
}

fn cmd_plot_lambdas(filepath: &PathBuf, bins: usize) -> Result<()> {
    println!("=== Lambda Distribution (bins: {}) ===", bins);
    println!("Filepath: {}", filepath.display());

    // This would load lambda values from a Lance dataset
    // For now, placeholder:
    println!("\n[Histogram visualization would appear here]");
    println!("(Requires trueno-viz integration)");

    // Example with trueno-viz (when integrated):
    /*
    use trueno_viz::prelude::*;
    use trueno_viz::output::{TerminalEncoder, TerminalMode};

    let lambdas = load_lambdas(filepath)?; // your loader

    let plot = Histogram::new()
        .data(&lambdas)
        .bins(bins)
        .build()?;

    let fb = plot.to_framebuffer()?;
    TerminalEncoder::new()
        .mode(TerminalMode::Ascii)
        .print(&fb);
    */

    Ok(())
}

fn cmd_plot_laplacian(filepath: &PathBuf, mode: &str) -> Result<()> {
    println!("=== Laplacian Plot (mode: {}) ===", mode);
    println!("Filepath: {}", filepath.display());

    println!("\n[Laplacian visualization would appear here]");
    println!("Mode: {}", mode);

    // With your existing sprs utilities:
    /*
    let laplacian = load_sparse_matrix(filepath)?;

    match mode {
        "density" => {
            use your_crate::introspection::visualisation::pretty_print_density_matrix;
            println!("{}", pretty_print_density_matrix(&laplacian));
        }
        "ascii" => {
            use your_crate::introspection::visualisation::pretty_print_ascii_matrix;
            println!("{}", pretty_print_ascii_matrix(&laplacian, Some("Laplacian")));
        }
        _ => println!("Unknown mode: {}", mode),
    }
    */

    Ok(())
}

fn cmd_clusters(filepath: &PathBuf) -> Result<()> {
    println!("=== Cluster Information ===");
    println!("Filepath: {}", filepath.display());

    // This would load cluster metadata
    println!("\n[Cluster visualization would appear here]");

    // With ArrowSpace metadata:
    /*
    let metadata = load_metadata(filepath)?;

    if let Some(n_clusters) = metadata.aspace_config.get("n_clusters") {
        println!("Number of clusters: {}", n_clusters);
        println!("Cluster radius: {}", metadata.cluster_radius);

        // Show cluster statistics
        for i in 0..n_clusters {
            let size = cluster_sizes[i];
            let mean_lambda = compute_cluster_lambda(i);
            println!("  Cluster {}: {} items, λ̄={:.4}", i, size, mean_lambda);
        }
    }
    */

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
