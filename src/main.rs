use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use tokio::runtime::Runtime;

use javelin::functions::*;
use javelin::{Cli, Command};

#[derive(Debug)]
#[allow(unused)]
enum AppError {
    Info(Error),
    Head(Error),
    Sample(Error),
    Stats(Error),
    Display(Error),
}

fn main() -> Result<()> {
    javelin::init();
    let args = Cli::parse();
    let filepath = args.filepath;

    // Single multi-threaded Tokio runtime for the whole CLI
    let rt = Runtime::new().expect("failed to create Tokio runtime");

    let _ = match args.cmd {
        Command::Info => rt.block_on(async {
            let _ = cmd_info(&filepath).await.map_err(AppError::Info);
        }),
        Command::Head { n } => rt.block_on(async {
            let _ = cmd_head(&filepath, n).await.map_err(AppError::Head);
        }),
        Command::Sample { n } => rt.block_on(async {
            let _ = cmd_sample(&filepath, n).await.map_err(AppError::Sample);
        }),
        Command::Stats => rt.block_on(async {
            let _ = cmd_stats(&filepath).await.map_err(AppError::Stats);
        }),
        // Command::PlotLambdas { bins } => cmd_plot_lambdas(&filepath, bins),
        // Command::PlotLaplacian { mode } => cmd_plot_laplacian(&filepath, &mode),
        // Command::Clusters => cmd_clusters(&filepath),
        Command::Tui => rt.block_on(async {
            let _ = run_tui(filepath);
        }),
        Command::Display => rt.block_on(async {
            let _ = cmd_display(&filepath).await.map_err(AppError::Display);
        }),
    };

    Ok(())
}
