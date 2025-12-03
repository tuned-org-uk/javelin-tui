pub(crate) mod display;
pub(crate) mod display_1d;
pub(crate) mod display_coo;
pub(crate) mod display_transposed;

/// Logical view of how a Lance dataset is stored.
///
/// - DenseRowMajor: { vector: FixedSizeList<Float64>[F] } – each row is a dense vector
/// - SparseCoo:     { row: UInt32, col: UInt32, value: Float64 } – COO triplets
/// - Vector1D:      single primitive column (e.g. lambdas, norms, indices)
/// - Other:         anything else; shown as‑is
pub enum LanceLayout {
    DenseRowMajor,
    SparseCoo,
    Vector1D,
    Other,
}
