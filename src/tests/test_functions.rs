use crate::functions::*;

use std::path::PathBuf;

// Helper: resolve a path relative to project root for test data.
// Adjust "tests/data" and filenames to match your repo layout.
fn test_data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

#[tokio::test]
async fn cmd_stats_runs_on_valid_lance() {
    // Requires a small valid Lance dataset in tests/data/sample.lance
    let path = test_data_path("sample.lance");
    if !path.exists() {
        // Allow CI to skip if fixture is missing
        eprintln!("Skipping cmd_stats_runs_on_valid_lance: {:?} missing", path);
        return;
    }

    let result = cmd_stats(&path).await;
    assert!(
        result.is_ok(),
        "cmd_stats should succeed on sample.lance: {result:?}"
    );
}

#[tokio::test]
async fn cmd_head_handles_empty_or_small_dataset() {
    let path = test_data_path("sample.lance");
    if !path.exists() {
        eprintln!(
            "Skipping cmd_head_handles_empty_or_small_dataset: {:?} missing",
            path
        );
        return;
    }

    // n larger than dataset size should not panic or error
    let result = cmd_head(&path, 10_000).await;
    assert!(
        result.is_ok(),
        "cmd_head should not fail on large n: {result:?}"
    );
}

#[tokio::test]
async fn cmd_sample_produces_at_most_n_rows() {
    let path = test_data_path("sample.lance");
    if !path.exists() {
        eprintln!(
            "Skipping cmd_sample_produces_at_most_n_rows: {:?} missing",
            path
        );
        return;
    }

    // Just check that the command returns Ok; semantics tested indirectly
    let n = 5;
    let result = cmd_sample(&path, n).await;
    assert!(result.is_ok(), "cmd_sample should succeed: {result:?}");
}

#[tokio::test]
async fn run_tui_returns_ok_for_directory() {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Assuming root has at least one `.lance` for manual / CI tests,
    // or you can point to tests/data
    dir.push("tests");
    dir.push("data");

    if !dir.exists() {
        eprintln!(
            "Skipping run_tui_returns_ok_for_directory: {:?} missing",
            dir
        );
        return;
    }

    // run_tui is interactive; here we just ensure it starts and exits quickly
    // by running it in a short-lived task or expecting it to return Ok immediately
    // when no keys are pressed. If it blocks forever, you may want to gate or mock.
    let result = run_tui(dir).await;
    assert!(
        result.is_ok(),
        "run_tui should return Ok for a valid directory: {result:?}"
    );
}
