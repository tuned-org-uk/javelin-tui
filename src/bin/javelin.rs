use anyhow::{Error, Result, anyhow};
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

use std::fmt;

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Info(e) => write!(f, "info command failed: {e}"),
            AppError::Head(e) => write!(f, "head command failed: {e}"),
            AppError::Sample(e) => write!(f, "sample command failed: {e}"),
            AppError::Stats(e) => write!(f, "stats command failed: {e}"),
            AppError::Display(e) => write!(f, "display command failed: {e}"),
            AppError::Tui(e) => write!(f, "tui command failed: {e}"),
            AppError::Generate(e) => write!(f, "generate command failed: {e}"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    use std::process::exit;

    javelin_tui::init();
    let args = Cli::parse();

    let rt = Runtime::new().expect("failed to create Tokio runtime");

    // Default to Tui when no subcommand is supplied
    let cmd = args.cmd.unwrap_or(Command::Tui);

    let result = match cmd {
        Command::Info => rt
            .block_on(async {
                let filepath = args
                    .filepath
                    .ok_or_else(|| anyhow!("--filepath is required for this command"))?;
                cmd_info(&filepath).await
            })
            .map_err(AppError::Info),
        Command::Head { n } => rt
            .block_on(async {
                let filepath = args
                    .filepath
                    .ok_or_else(|| anyhow!("--filepath is required for this command"))?;
                cmd_head(&filepath, n).await
            })
            .map_err(AppError::Head),
        Command::Sample { n } => rt
            .block_on(async {
                let filepath = args
                    .filepath
                    .ok_or_else(|| anyhow!("--filepath is required for this command"))?;
                cmd_sample(&filepath, n).await
            })
            .map_err(AppError::Sample),
        Command::Stats => rt
            .block_on(async {
                let filepath = args
                    .filepath
                    .ok_or_else(|| anyhow!("--filepath is required for this command"))?;
                cmd_stats(&filepath).await
            })
            .map_err(AppError::Stats),
        Command::Tui => rt
            .block_on(async {
                let filepath = args
                    .filepath
                    .ok_or_else(|| anyhow!("--filepath is required for this command"))?;
                run_tui(filepath).await
            })
            .map_err(AppError::Tui),
        Command::Display => rt
            .block_on(async {
                let filepath = args
                    .filepath
                    .ok_or_else(|| anyhow!("--filepath is required for this command"))?;
                cmd_display(&filepath).await
            })
            .map_err(AppError::Display),
        Command::Generate {
            n_items,
            n_dims,
            seed,
        } => rt
            .block_on(async {
                println!("Generating sample dataset in ./test_javelin");
                cmd_generate(n_items, n_dims, seed).await
            })
            .map_err(AppError::Generate),
    };

    if let Err(e) = result {
        eprintln!("Error: {e:?}");
        exit(1);
    }
    Ok(())
}
