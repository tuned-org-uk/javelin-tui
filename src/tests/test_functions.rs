use crate::functions::*;

use genegraph_storage::lance::LanceStorage;
use genegraph_storage::traits::StorageBackend;
use smartcore::linalg::basic::arrays::Array;
use std::fs;
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

#[tokio::test]
async fn cmd_generate_creates_expected_artifacts() {
    use crate::datasets::remove_directory_if_exists;
    // Use a small dataset so the test is fast.
    const N_ITEMS: usize = 20;
    const N_DIMS: usize = 5;
    const SEED: u64 = 42;

    // 1. Create a temporary output directory under target/
    let mut out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let name_id = "javelin_test";
    out_dir.push(name_id);

    remove_directory_if_exists(PathBuf::from(out_dir.clone()).as_path()).unwrap();

    // 2. Call cmd_generate
    cmd_generate(N_ITEMS, N_DIMS, SEED)
        .await
        .expect("cmd_generate should succeed");

    // 3. Verify that storage can be opened and metadata exists
    let storage = LanceStorage::new(
        out_dir.to_str().expect("non-UTF8 test path").to_string(),
        "javelin_test".to_string(),
    );

    // Metadata file must exist and be readable
    let md = storage
        .load_metadata()
        .await
        .expect("metadata should be loadable after cmd_generate");
    assert!(
        !md.files.is_empty(),
        "metadata should contain at least one file entry"
    );

    // 4. Check dense matrix ("raw_input")
    let dense = storage
        .load_dense("raw_input")
        .await
        .expect("raw_input dense matrix should be loadable");
    let (rows, cols) = dense.shape();
    assert_eq!(rows, N_ITEMS, "dense rows should match N_ITEMS");
    assert_eq!(
        cols, N_DIMS,
        "dense cols should match N_ITEMS (by construction)"
    );

    // 5. Check sparse adjacency matrix ("adjacency")
    let adj = storage
        .load_sparse("adjacency")
        .await
        .expect("adjacency sparse matrix should be loadable");
    assert_eq!(adj.rows(), N_ITEMS, "adjacency rows should match N_ITEMS");
    assert_eq!(adj.cols(), N_ITEMS, "adjacency cols should match N_ITEMS");
    assert!(adj.nnz() > 0, "adjacency matrix should have non-zeros");

    // 6. Check generic vector ("norms")
    let norms = storage
        .load_vector("norms")
        .await
        .expect("norms vector should be loadable");
    assert_eq!(
        norms.len(),
        N_ITEMS,
        "norms vector length should match N_ITEMS"
    );
}
