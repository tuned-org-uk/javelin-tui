use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use tokio::runtime::Runtime;

use javelin_tui::functions::*;
use javelin_tui::{Cli, Command};

#[derive(Debug)]
#[allow(unused)]
enum AppError {
    Info(Error),
    Head(Error),
    Sample(Error),
    Stats(Error),
    Display(Error),
    Tui(Error),
    Generate(Error),
}

fn main() -> anyhow::Result<()> {
    use std::process::exit;

    javelin_tui::init();
    let args = Cli::parse();
    let filepath = args.filepath;

    let rt = Runtime::new().expect("failed to create Tokio runtime");

    // Default to Tui when no subcommand is supplied
    let cmd = args.cmd.unwrap_or(Command::Tui);

    let result = match cmd {
        Command::Info => rt
            .block_on(async { cmd_info(&filepath).await })
            .map_err(AppError::Info),
        Command::Head { n } => rt
            .block_on(async { cmd_head(&filepath, n).await })
            .map_err(AppError::Head),
        Command::Sample { n } => rt
            .block_on(async { cmd_sample(&filepath, n).await })
            .map_err(AppError::Sample),
        Command::Stats => rt
            .block_on(async { cmd_stats(&filepath).await })
            .map_err(AppError::Stats),
        Command::Tui => rt
            .block_on(async { run_tui(filepath).await })
            .map_err(AppError::Tui),
        Command::Display => rt
            .block_on(async { cmd_display(&filepath).await })
            .map_err(AppError::Display),
        Command::Generate {
            n_items,
            n_dims,
            seed,
        } => rt
            .block_on(async { cmd_generate(n_items, n_dims, seed).await })
            .map_err(AppError::Generate),
    };

    if let Err(e) = result {
        eprintln!("Error: {e:?}");
        exit(1);
    }
    Ok(())
}
