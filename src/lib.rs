pub mod datasets;
pub mod display;
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
#[command(name = "javelin", about = "Display and work with Lance matrices")]
pub struct Cli {
    /// Path to a lance file or directory
    #[arg(long)]
    pub filepath: Option<PathBuf>,
    #[command(subcommand)]
    pub cmd: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    Tui,
    Info,
    Head {
        n: usize,
    },
    Sample {
        n: usize,
    },
    Stats,
    Display,
    Generate {
        #[arg(long, default_value = "200")]
        n_items: usize,
        #[arg(long, default_value = "300")]
        n_dims: usize,
        #[arg(long, default_value = "42")]
        seed: u64,
    },
}
