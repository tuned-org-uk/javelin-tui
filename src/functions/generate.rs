use log::{debug, info};
use smartcore::linalg::basic::arrays::Array2;
use std::path::PathBuf;

/// Generate a toy dataset to showcase
pub async fn cmd_generate(n_items: usize, n_dims: usize, seed: u64) -> anyhow::Result<()> {
    use crate::datasets::make_gaussian_cliques_multi;
    use genegraph_storage::lance::LanceStorage;
    use genegraph_storage::metadata::GeneMetadata;
    use genegraph_storage::traits::StorageBackend;
    use smartcore::linalg::basic::matrix::DenseMatrix;

    // 1) Prepare storage directory and minimal metadata
    let mut out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let name_id = "javelin_test";
    out_dir.push(name_id);
    if out_dir.exists() {
        std::fs::remove_dir_all(&out_dir).unwrap();
    }

    let storage = LanceStorage::new(
        out_dir.to_str().expect("non-UTF8 test path").to_string(),
        "javelin_test".to_string(),
    );

    // 2) Generate dense “full” dataset
    let (dense, sparse, vector) = make_gaussian_cliques_multi(n_items, 0.3, 5, n_dims, seed);
    let (nitems, nfeatures) = (dense.len(), dense[0].len());

    // Create metadata
    GeneMetadata::seed_metadata(&name_id, nitems, nfeatures, &storage)
        .await
        .unwrap();
    debug!("Saving metadata first to initialize storage directory");

    // add data to the storage
    let dense_matrix =
        DenseMatrix::<f64>::from_iterator(dense.iter().flatten().map(|x| *x), nitems, nfeatures, 0);
    storage
        .save_dense("raw_input", &dense_matrix, &storage.metadata_path())
        .await?;

    // the adjacency file and the norms file
    let mut md: GeneMetadata = storage.load_metadata().await.unwrap();
    let mock_info_adj = md.new_fileinfo(
        "adjacency",
        "sparse",
        (nitems, nitems),
        Some(sparse.nnz()),
        None,
    );
    let mock_info_norms = md.new_fileinfo("norms", "vector", (nitems, 1), None, None);

    md = md.add_file("adjacency", mock_info_adj);
    md = md.add_file("norms", mock_info_norms);

    storage
        .save_sparse("adjacency", &sparse, &storage.metadata_path())
        .await
        .unwrap();
    storage
        .save_vector("norms", &vector.as_slice(), &storage.metadata_path())
        .await
        .unwrap();

    // Fill any required fields on md.aspace_config, etc.
    storage.save_metadata(&md).await?;

    println!(
        "Generated example datasets in {:?}:
  - dense Lance:   {} rows × {} cols (raw_input)
  - sparse Lance:  (adjacency)
  - 1D vector Lance: (norms)",
        out_dir, nitems, nfeatures,
    );
    info!("Try now `javelin --filepath ./javelin_test`");

    Ok(())
}
