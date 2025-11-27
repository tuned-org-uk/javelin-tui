pub mod display;
pub mod display_transposed;
pub mod functions;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "javelin", about = "Lance inspector")]
pub struct Cli {
    /// Path to the Lance file
    #[arg(long)]
    pub filepath: PathBuf,
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Info,
    Head { n: usize },
    Sample { n: usize },
    Stats,
    // PlotLambdas {
    //     #[arg(long, default_value = "64")]
    //     bins: usize,
    // },
    // PlotLaplacian {
    //     #[arg(long, default_value = "density")]
    //     mode: String,
    // },
    // Clusters,
    Tui,
}
