pub mod display;
pub mod display_1d;
pub mod display_coo;
pub mod display_transposed;
pub mod functions;

#[cfg(test)]
mod tests;

use std::sync::Once;

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(|| {
        // Read RUST_LOG env variable, default to "info" if not set
        let env = env_logger::Env::default().default_filter_or("info");

        // don't panic if called multiple times across binaries
        let _ = env_logger::Builder::from_env(env).try_init();
    });
}

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "javelin", about = "Lance inspector")]
pub struct Cli {
    /// Path to a lance file or directory
    #[arg(long)]
    pub filepath: PathBuf,
    #[command(subcommand)]
    pub cmd: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    Tui,
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
    Display,
}
