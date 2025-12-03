use anyhow::Result;
use lance::Dataset;
use std::path::PathBuf;

use crate::datasets::path_to_uri;
use crate::display::display::display_spreadsheet_interactive;
use crate::functions::functions::normalize_for_display;

pub async fn cmd_head(filepath: &PathBuf, n: usize) -> Result<()> {
    let uri = path_to_uri(filepath);
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

    let batch = normalize_for_display(&batch)?;
    display_spreadsheet_interactive(&batch)?;
    Ok(())
}
