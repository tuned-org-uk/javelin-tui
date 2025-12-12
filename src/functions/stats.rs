use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::DataType;
use lance::Dataset;
use std::path::PathBuf;

use crate::datasets::path_to_uri;

pub async fn cmd_stats(filepath: &PathBuf) -> Result<()> {
    println!("=== Dataset Statistics ===\n");

    let uri = path_to_uri(filepath);
    let dataset = Dataset::open(&uri).await?;
    let schema = dataset.schema();
    let count = dataset.count_rows(None).await?;

    println!("Total rows: {}", count);
    println!("Total columns: {}\n", schema.fields.len());

    // Sample first 1000 rows for statistics
    let sample_size = 1000.min(count);
    let mut scanner = dataset.scan();
    let batch = scanner
        .limit(Some(sample_size as i64), None)?
        .try_into_batch()
        .await?;

    println!("Column details (based on {} sample rows):\n", sample_size);

    for (idx, field) in schema.fields.iter().enumerate() {
        println!("  • Column: {}", field.name);
        println!("    Type: {}", format_data_type(&field.data_type()));
        println!("    Nullable: {}", field.nullable);

        let col = batch.column(idx);

        // Detect data structure type
        match detect_structure(col, &field.data_type()) {
            DataStructure::Vector1D(size) => {
                println!("    Structure: 1D Vector (size {})", size);
                calculate_vector_stats(col);
            }
            DataStructure::Matrix2D(rows, cols) => {
                println!("    Structure: 2D Matrix ({}×{})", rows, cols);
                calculate_matrix_stats(col);
            }
            DataStructure::DenseMatrix(rows, cols) => {
                println!("    Structure: 2D Dense Matrix ({}×{})", rows, cols);
                calculate_dense_matrix_stats(col, rows, cols);
            }
            DataStructure::SparseMatrix => {
                println!("    Structure: Sparse Matrix (COO/CSR format)");
                calculate_sparse_matrix_stats(col);
            }
            DataStructure::Scalar => {
                println!("    Structure: Scalar value");
                calculate_scalar_stats(col);
            }
            DataStructure::Other => {
                println!("    Structure: Other/Complex");
            }
        }

        println!();
    }

    Ok(())
}

#[derive(Debug)]
enum DataStructure {
    Vector1D(i32),         // 1D vector with size
    Matrix2D(i32, i32),    // 2D matrix (rows, cols)
    DenseMatrix(i32, i32), // Dense 2D matrix stored as flattened
    SparseMatrix,          // Sparse matrix (COO or CSR)
    Scalar,                // Single value
    Other,                 // Complex or unknown structure
}

fn detect_structure(col: &ArrayRef, data_type: &DataType) -> DataStructure {
    match data_type {
        // Dense row-major matrix: each row is a FixedSizeList of scalars
        DataType::FixedSizeList(inner, size) => {
            match inner.data_type() {
                DataType::Float32 | DataType::Float64 | DataType::Int32 | DataType::Int64 => {
                    // This is actually a dense matrix in row-major format
                    DataStructure::DenseMatrix(1, *size) // 1 row per record, size columns
                }
                // 3D or higher: FixedSizeList of FixedSizeList
                DataType::FixedSizeList(inner2, cols) => DataStructure::Matrix2D(*size, *cols),
                _ => DataStructure::Other,
            }
        }

        // Check for struct-based sparse representation
        DataType::Struct(fields) => {
            let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();

            // COO format: has row_indices, col_indices, values
            if field_names.contains(&"row_indices")
                && field_names.contains(&"col_indices")
                && field_names.contains(&"values")
            {
                return DataStructure::SparseMatrix;
            }

            // CSR format: has indptr, indices, data
            if field_names.contains(&"indptr")
                && field_names.contains(&"indices")
                && field_names.contains(&"data")
            {
                return DataStructure::SparseMatrix;
            }

            DataStructure::Other
        }

        // Variable-length list (could be sparse or ragged)
        DataType::List(_) => DataStructure::Other,

        // Scalar types
        DataType::Float32
        | DataType::Float64
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::Boolean => DataStructure::Scalar,

        _ => DataStructure::Other,
    }
}

fn calculate_vector_stats(col: &ArrayRef) {
    if let Some(list_array) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = list_array.values();

        if let Some(stats) = calculate_numeric_stats(values.as_ref()) {
            println!("    Vector element statistics:");
            println!("      Mean:   {:.6}", stats.mean);
            println!("      Std:    {:.6}", stats.std);
            println!("      Min:    {:.6}", stats.min);
            println!("      Max:    {:.6}", stats.max);
            println!("      Nulls:  {}", stats.null_count);
        }
    }
}

fn calculate_matrix_stats(col: &ArrayRef) {
    if let Some(outer_list) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        let inner_list_ref = outer_list.values();

        if let Some(inner_list) = inner_list_ref.as_any().downcast_ref::<FixedSizeListArray>() {
            let values = inner_list.values();

            if let Some(stats) = calculate_numeric_stats(values.as_ref()) {
                println!("    Matrix element statistics:");
                println!("      Mean:   {:.6}", stats.mean);
                println!("      Std:    {:.6}", stats.std);
                println!("      Min:    {:.6}", stats.min);
                println!("      Max:    {:.6}", stats.max);
                println!("      Nulls:  {}", stats.null_count);
            }
        }
    }
}

fn calculate_dense_matrix_stats(col: &ArrayRef, _rows: i32, cols: i32) {
    if let Some(list_array) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = list_array.values();
        let num_records = list_array.len();

        if let Some(stats) = calculate_numeric_stats(values.as_ref()) {
            println!("    Dense matrix representation:");
            println!("      Shape: {} records × {} features", num_records, cols);
            println!(
                "      Storage: Row-major (each record is a {}-dim vector)",
                cols
            );
            println!("    Element statistics:");
            println!("      Mean:   {:.6}", stats.mean);
            println!("      Std:    {:.6}", stats.std);
            println!("      Min:    {:.6}", stats.min);
            println!("      Max:    {:.6}", stats.max);
            println!("      Nulls:  {}", stats.null_count);
        }
    }
}

fn calculate_sparse_matrix_stats(col: &ArrayRef) {
    if let Some(struct_array) = col.as_any().downcast_ref::<StructArray>() {
        // Calculate sparsity
        let mut total_nnz = 0;
        let mut sample_count = 0;

        // Try to get values/data field
        for field_name in &["values", "data"] {
            if let Some(values_col) = struct_array.column_by_name(field_name) {
                if let Some(list_array) = values_col.as_any().downcast_ref::<ListArray>() {
                    for i in 0..list_array.len() {
                        if !list_array.is_null(i) {
                            let value_array = list_array.value(i);
                            total_nnz += value_array.len();
                            sample_count += 1;
                        }
                    }
                }

                if sample_count > 0 {
                    let avg_nnz = total_nnz as f64 / sample_count as f64;
                    println!("    Sparse matrix statistics:");
                    println!("      Avg non-zeros per sample: {:.2}", avg_nnz);
                    println!("      Total samples analyzed: {}", sample_count);

                    // Calculate stats on non-zero values
                    if let Some(list_array) = values_col.as_any().downcast_ref::<ListArray>() {
                        let mut all_values = Vec::new();
                        for i in 0..list_array.len().min(100) {
                            if !list_array.is_null(i) {
                                let value_array = list_array.value(i);
                                if let Some(float_array) =
                                    value_array.as_any().downcast_ref::<Float64Array>()
                                {
                                    for j in 0..float_array.len() {
                                        if !float_array.is_null(j) {
                                            all_values.push(float_array.value(j));
                                        }
                                    }
                                }
                            }
                        }

                        if !all_values.is_empty() {
                            let mean = all_values.iter().sum::<f64>() / all_values.len() as f64;
                            let min = all_values.iter().cloned().fold(f64::INFINITY, f64::min);
                            let max = all_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

                            println!("      Non-zero value statistics:");
                            println!("        Mean: {:.6}", mean);
                            println!("        Min:  {:.6}", min);
                            println!("        Max:  {:.6}", max);
                        }
                    }
                }
                break;
            }
        }
    }
}

fn calculate_scalar_stats(col: &ArrayRef) {
    if let Some(stats) = calculate_numeric_stats(col) {
        println!("    Scalar statistics:");
        println!("      Mean:   {:.6}", stats.mean);
        println!("      Std:    {:.6}", stats.std);
        println!("      Min:    {:.6}", stats.min);
        println!("      Max:    {:.6}", stats.max);
        println!("      Nulls:  {}", stats.null_count);
    } else if let Some(string_array) = col.as_any().downcast_ref::<StringArray>() {
        let null_count = string_array.null_count();
        let total_len: usize = (0..string_array.len())
            .filter(|&i| !string_array.is_null(i))
            .map(|i| string_array.value(i).len())
            .sum();
        let non_null = string_array.len() - null_count;

        if non_null > 0 {
            println!("    String statistics:");
            println!(
                "      Avg length: {:.2}",
                total_len as f64 / non_null as f64
            );
            println!("      Nulls: {}", null_count);
        }
    }
}

struct NumericStats {
    mean: f64,
    std: f64,
    min: f64,
    max: f64,
    null_count: usize,
}

fn calculate_numeric_stats(array: &dyn Array) -> Option<NumericStats> {
    let mut sum = 0.0;
    let mut sum_sq = 0.0;
    let mut min_val = f64::INFINITY;
    let mut max_val = f64::NEG_INFINITY;
    let mut count = 0;
    let null_count = array.null_count();

    // Try different numeric types
    if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
        for i in 0..float_array.len() {
            if !float_array.is_null(i) {
                let val = float_array.value(i);
                sum += val;
                sum_sq += val * val;
                min_val = min_val.min(val);
                max_val = max_val.max(val);
                count += 1;
            }
        }
    } else if let Some(float_array) = array.as_any().downcast_ref::<Float32Array>() {
        for i in 0..float_array.len() {
            if !float_array.is_null(i) {
                let val = float_array.value(i) as f64;
                sum += val;
                sum_sq += val * val;
                min_val = min_val.min(val);
                max_val = max_val.max(val);
                count += 1;
            }
        }
    } else if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
        for i in 0..int_array.len() {
            if !int_array.is_null(i) {
                let val = int_array.value(i) as f64;
                sum += val;
                sum_sq += val * val;
                min_val = min_val.min(val);
                max_val = max_val.max(val);
                count += 1;
            }
        }
    } else if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
        for i in 0..int_array.len() {
            if !int_array.is_null(i) {
                let val = int_array.value(i) as f64;
                sum += val;
                sum_sq += val * val;
                min_val = min_val.min(val);
                max_val = max_val.max(val);
                count += 1;
            }
        }
    } else {
        return None;
    }

    if count == 0 {
        return None;
    }

    let mean = sum / count as f64;
    let variance = (sum_sq / count as f64) - (mean * mean);
    let std = variance.max(0.0).sqrt();

    Some(NumericStats {
        mean,
        std,
        min: min_val,
        max: max_val,
        null_count,
    })
}

fn format_data_type(dt: &DataType) -> String {
    match dt {
        DataType::FixedSizeList(inner, size) => {
            format!(
                "FixedSizeList<{}, {}>",
                format_data_type(inner.data_type()),
                size
            )
        }
        DataType::List(inner) => {
            format!("List<{}>", format_data_type(inner.data_type()))
        }
        DataType::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), format_data_type(&f.data_type())))
                .collect();
            format!("Struct<{}>", field_strs.join(", "))
        }
        DataType::Float32 => "Float32".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::Int8 => "Int8".to_string(),
        DataType::Int16 => "Int16".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::UInt8 => "UInt8".to_string(),
        DataType::UInt16 => "UInt16".to_string(),
        DataType::UInt32 => "UInt32".to_string(),
        DataType::UInt64 => "UInt64".to_string(),
        DataType::Utf8 => "String".to_string(),
        DataType::LargeUtf8 => "LargeString".to_string(),
        DataType::Boolean => "Boolean".to_string(),
        DataType::Binary => "Binary".to_string(),
        _ => format!("{:?}", dt),
    }
}
