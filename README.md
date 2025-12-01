#           _     ___ __      __ ______  _       _____  _   _                                                          
#          | |   /   \\ \    / /|  ____|| |     |_   _|| \ | |                                                         
#          | |  /  ^  \\ \  / / | |__   | |       | |  |  \| |                                                         
#      _   | | /  /_\  \\ \/ /  |  __|  | |       | |  | . ` |                                                         
#     | |__| |/  _____  \\  /   | |____ | |____  _| |_ | |\  |                                                         
#      \____//__/     \__\\/    |______||______||_____||_| \_|                                                         
#            |__|     |__|                                                                                             
                                                                                                                   
#                                
#    vector  space  visualisation
#                                                                                                                             
                                                                                                                      
                                                                                                                      
Lance inspector and TUI for Arrow/Lance datasets.

Javelin is a Rust-based command-line and TUI tool for inspecting datasets stored in the Lance format (and compatible Parquet exports). It focuses on fast, ergonomic exploration of embedding-like matrices, sparse COO data, and 1D vectors using an interactive terminal UI built on `ratatui` and `crossterm`.

Installation:
```bash
cargo install javelin-tui
```

---

## Features

### Single-entry TUI launcher

- `javelin --filepath /path/to/dir` starts a launcher that:
  - Scans a directory for `.lance` datasets.
  - Lets you select a file with **Up/Down** keys.
  - Lets you select a command (**Head**, **Sample**, **Display**, etc.) with **Left/Right** keys.
  - Launches the corresponding interactive viewer for the chosen file.

If no subcommand is provided, the default is the TUI launcher.

### Dense matrix viewer

- Detects Lance “vector” layout and generic dense `col_*` layouts.
- Shows a scrollable table with:
  - A row index column.
  - Feature columns from `col_*`.
  - Per-row **mean** and **standard deviation** (for multi-column dense layouts).
- Supports:
  - Horizontal scrolling over features.
  - Vertical scrolling over rows.
  - A transposed view (features × samples) toggled via a key.

### 1D vector viewer

- Specialized UI for `LanceLayout::Vector1D` data (e.g. eigenvalues, norms).
- No avg/std columns; values are displayed with **12 decimal digits**.
- Same navigation shortcuts as the dense viewer.

### Sparse COO viewer

- Expects COO data in `row`, `col`, `value` schema.
- Layout:
  - **Top**: matrix metadata and density.
  - **Middle**:
    - Triples table with vertical scrolling over `(row, col, value)` entries.
    - ASCII sparsity map (downsampled for large matrices) that highlights nonzeros.
  - **Bottom**: diagonals and connectivity summaries (e.g., most connected rows).

### Sampling and indexing

- `cmd_sample`:
  - Randomly selects `n` distinct row indices.
  - Reads the minimal prefix needed to cover those indices.
  - Uses Arrow `take` to build a sampled `RecordBatch`.
  - Adds a `row_idx` column with the **original dataset indices**.
  - Opens the sampled batch in the TUI viewer.

- `cmd_head`:
  - Shows the first `n` rows in the interactive viewer.

- `cmd_stats`:
  - Reports dataset row count and schema.
  - Prints per-column structural information.

### Storage integration

- Uses a `LanceStorage` backend to:
  - Load dense matrices from Lance vector datasets.
  - Load sparse COO matrices from Lance triplet datasets.
  - Save dense matrices back as Lance vector datasets via `save_dense("raw_input")`.

- Parquet support:
  - Reads Parquet into Arrow `RecordBatch`es.
  - Detects:
    - Lance-like vector format (`FixedSizeList<Float64>`), or
    - Wide columnar float format with `col_*` columns.
  - Converts wide columnar Parquet to a dense matrix and saves it as Lance row‑major “raw_input” using `save_dense`.

---

## Installation

### Prerequisites

- Rust (stable toolchain and `cargo`).
- A terminal that supports ANSI escape codes.

### Build from source

```
git clone https://gitlab.com/yera/javelin.git
cd javelin
cargo build --release
```

The binary will be available at:

```
target/release/javelin
```

---

## Usage

### Basic CLI

```

# Print schema and statistics
javelin --filepath /path/to/dataset.lance stats

# Show the first 20 rows in the TUI viewer
javelin --filepath /path/to/dataset.lance head --n 20

# Randomly sample 50 rows, preserving original indices, and open in TUI
javelin --filepath /path/to/dataset.lance sample --n 50

# Open full dataset in TUI viewer
javelin --filepath /path/to/dataset.lance display
```

### TUI launcher (default)

```
# Launcher for a directory of Lance datasets
javelin --filepath /path/to/dir
```

If you omit the subcommand:

- When `filepath` is a directory, the launcher scans it for `.lance` files.
- When `filepath` is a file, you can still use the launcher to choose a command.

#### Launcher key bindings

- **Up / Down** or **k / j**: Move selection between files.
- **Left / Right** or **h / l**: Cycle between commands (Head, Sample, Display, …).
- **Enter**: Run the selected command on the selected file.
- **q / Esc**: Exit the launcher.

When you press **Enter**:

- The launcher tears down its own TUI.
- Runs the chosen command (e.g. display, sample) on the chosen file.
- Restores the launcher when the command’s viewer exits.

---

## Interactive viewers

### Dense and 1D viewers

Key bindings:
- **Up / Down** or **k / j**:
  - Scroll vertically over rows.
- **Left / Right** or **h / l**:
  - Scroll horizontally over feature columns (dense) or vector columns (1D).
- **H**:
  - Jump to the first visible column.
- **E**:
  - Jump to the last visible column window.
- **t**:
  - Toggle transpose (N×F ↔ F×N) in dense layouts.
- **q / Esc**:
  - Exit the viewer.

Behavior:
- Dense layouts show:
  - Synthetic “Row” index column.
  - `col_*` features.
  - Per-row `avg` and `std` computed over all numeric feature columns.

- 1D layouts show:
  - Row index.
  - One or more value columns with 12 decimal digits and no avg/std.

### Sparse COO viewer

Key bindings are the same for scrolling:

- **Up / Down** or **k / j**: vertical scroll through triples.
- **q / Esc**: exit.

Panels:

- **Metadata**: matrix dimensions and density.
- **Triples table**: index, `row`, `col`, `value` with vertical scrolling.
- **Sparsity map**: ASCII grid marking nonzeros.
- **Structure summary**: main diagonal entries and most-connected rows.

---

## Data formats

### Dense Lance (vector) format

- Stored as a single `FixedSizeList<Float64>` column (e.g. `vector`).
- Reconstructed as a dense matrix in column-major order for computation.
- Displayed in the TUI as:
  - A dense matrix table, or
  - A 1D vector viewer for `LanceLayout::Vector1D`.

### Sparse COO format

- Schema:

```
row:   UInt32
col:   UInt32
value: Float64
```

- Matrix dimensions stored in schema metadata (`rows`, `cols`, `nnz`).
- Reconstructed internally as a CSR matrix when needed.

### Parquet import

- Vector-like: `FixedSizeList<Float64>` column(s).
- Wide columnar: multiple `Float64` `col_*` columns.
- Wide columnar data is:
- Interpreted as a dense matrix.
- Saved via `save_dense("raw_input")` into Lance format.
- Then handled by the same TUI paths as native Lance data.


## Typical workflows

### Inspect an embedding matrix

```
javelin --filepath embeddings.lance display
```

- Scroll across feature dimensions.
- Toggle transpose to view feature-centric slices.
- Inspect per-row `avg` and `std`.

### Random sample with original indices

```
javelin --filepath embeddings.lance sample --n 100
```

- Examine the `row_idx` column to see which original rows were sampled.

### Explore a directory of experiments

```
javelin --filepath ./experiments
```

- Pick a dataset with Up/Down.
- Choose **Head**, **Sample**, or **Display** with Left/Right.
- Press Enter to launch the viewer.

### Inspect sparse matrices

```
javelin --filepath graph.lance display
```

- Use the COO viewer to inspect structure, sparsity pattern, and connectivity.

---

## License

See the `LICENSE` file in this repository for licensing details. Respect all third-party library licenses when redistributing binaries or integrating Javelin into other systems.
