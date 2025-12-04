pub(crate) mod display;
pub(crate) mod display_1d;
pub(crate) mod display_coo;
pub(crate) mod display_sparse_viz;
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

// === Color Definitions =====================================================
use ratatui::style::Color;

// Alternating row background colors
const EVEN_ROW_BG: Color = Color::Rgb(40, 42, 54);
const ODD_ROW_BG: Color = Color::Rgb(50, 52, 64);

// Alternating column background colors
const EVEN_COL_BG: Color = Color::Rgb(44, 46, 58);
const ODD_COL_BG: Color = Color::Rgb(54, 56, 68);

// Header colors
const HEADER_FG: Color = Color::Rgb(255, 184, 108); // Warm orange
const HEADER_BG: Color = Color::Rgb(68, 71, 90);

// Text colors
const TEXT_PRIMARY: Color = Color::Rgb(248, 248, 242); // Off-white
const TEXT_SECONDARY: Color = Color::Rgb(139, 233, 253); // Cyan
const TEXT_ACCENT: Color = Color::Rgb(80, 250, 123); // Green

// Border colors
const BORDER_PRIMARY: Color = Color::Rgb(98, 114, 164); // Blue-purple
const BORDER_ACCENT: Color = Color::Rgb(139, 233, 253); // Cyan

// Sparse visualization colors
const SPARSE_ASTERISK: Color = Color::Rgb(255, 121, 198); // Hot pink
const SPARSE_DOT: Color = Color::Rgb(68, 71, 90); // Dark gray
const SPARSE_BORDER: Color = Color::Rgb(80, 250, 123); // Green
