//! Graph connectivity visualization for sparse matrices
//!
//! This module provides functions to visualize sparse matrix connectivity
//! as a graph, showing relationships between nodes (rows) based on shared
//! non-zero entries in columns.

use anyhow::{Result, anyhow};
use arrow::array::{Float64Array, UInt32Array};
use arrow::record_batch::RecordBatch;
use arrow_array::Array;
use std::collections::{HashMap, HashSet};

/// Represents a node in the connectivity graph
#[derive(Debug, Clone)]
pub struct GraphNode {
    pub id: usize,
    pub degree: usize,
    pub connected_to: Vec<usize>,
}

/// Represents an edge in the connectivity graph
#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub from: usize,
    pub to: usize,
    pub weight: f64, // Number of shared columns
}

/// Graph connectivity structure derived from sparse matrix
#[derive(Debug)]
pub struct ConnectivityGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub n_rows: usize,
    pub n_cols: usize,
}

impl ConnectivityGraph {
    /// Build a connectivity graph from a COO sparse matrix
    ///
    /// Two rows are connected if they share at least one column with non-zero values.
    /// Edge weight represents the number of shared columns.
    pub fn from_coo_batch(batch: &RecordBatch) -> Result<Self> {
        let schema = batch.schema();

        // Locate row/col/value columns
        let mut row_idx = None;
        let mut col_idx = None;
        let mut val_idx = None;

        for (i, field) in schema.fields().iter().enumerate() {
            match field.name().as_str() {
                "row" => row_idx = Some(i),
                "col" => col_idx = Some(i),
                "value" => val_idx = Some(i),
                _ => {}
            }
        }

        let (row_i, col_i, val_i) = match (row_idx, col_idx, val_idx) {
            (Some(r), Some(c), Some(v)) => (r, c, v),
            _ => {
                return Err(anyhow!(
                    "COO schema must contain 'row', 'col', and 'value' columns"
                ));
            }
        };

        let row_arr = batch
            .column(row_i)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| anyhow!("row must be UInt32"))?;

        let col_arr = batch
            .column(col_i)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| anyhow!("col must be UInt32"))?;

        let val_arr = batch
            .column(val_i)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow!("value must be Float64"))?;

        let nnz = row_arr.len();

        // Determine matrix dimensions
        let md = schema.metadata();
        let n_rows = md
            .get("rows")
            .and_then(|r| r.parse::<usize>().ok())
            .unwrap_or_else(|| row_arr.iter().filter_map(|x| x).max().unwrap_or(0) as usize + 1);

        let n_cols = md
            .get("cols")
            .and_then(|c| c.parse::<usize>().ok())
            .unwrap_or_else(|| col_arr.iter().filter_map(|x| x).max().unwrap_or(0) as usize + 1);

        // Build row -> columns mapping
        let mut row_to_cols: HashMap<usize, HashSet<usize>> = HashMap::new();

        for i in 0..nnz {
            if val_arr.is_null(i) {
                continue;
            }
            let row = row_arr.value(i) as usize;
            let col = col_arr.value(i) as usize;
            row_to_cols
                .entry(row)
                .or_insert_with(HashSet::new)
                .insert(col);
        }

        // Build connectivity graph
        let mut edges = Vec::new();
        let mut node_degrees: HashMap<usize, usize> = HashMap::new();

        // Find shared columns between rows
        for (&row1, cols1) in row_to_cols.iter() {
            for (&row2, cols2) in row_to_cols.iter() {
                if row1 >= row2 {
                    continue; // Avoid duplicates and self-loops
                }

                // Count shared columns
                let shared: HashSet<_> = cols1.intersection(cols2).collect();
                let weight = shared.len();

                if weight > 0 {
                    edges.push(GraphEdge {
                        from: row1,
                        to: row2,
                        weight: weight as f64,
                    });

                    *node_degrees.entry(row1).or_insert(0) += 1;
                    *node_degrees.entry(row2).or_insert(0) += 1;
                }
            }
        }

        // Build nodes with connectivity info
        let mut nodes = Vec::new();
        for row in 0..n_rows {
            let connected_to: Vec<usize> = edges
                .iter()
                .filter_map(|e| {
                    if e.from == row {
                        Some(e.to)
                    } else if e.to == row {
                        Some(e.from)
                    } else {
                        None
                    }
                })
                .collect();

            nodes.push(GraphNode {
                id: row,
                degree: node_degrees.get(&row).copied().unwrap_or(0),
                connected_to,
            });
        }

        Ok(Self {
            nodes,
            edges,
            n_rows,
            n_cols,
        })
    }

    /// Get the most connected nodes (hubs)
    pub fn get_hubs(&self, top_k: usize) -> Vec<&GraphNode> {
        let mut sorted_nodes: Vec<&GraphNode> = self.nodes.iter().collect();
        sorted_nodes.sort_by(|a, b| b.degree.cmp(&a.degree));
        sorted_nodes.into_iter().take(top_k).collect()
    }

    /// Get isolated nodes (no connections)
    pub fn get_isolated_nodes(&self) -> Vec<&GraphNode> {
        self.nodes.iter().filter(|n| n.degree == 0).collect()
    }

    /// Get strongly connected components (simplified version)
    pub fn connected_components(&self) -> Vec<Vec<usize>> {
        let mut visited = vec![false; self.n_rows];
        let mut components = Vec::new();

        for node in &self.nodes {
            if visited[node.id] {
                continue;
            }

            let mut component = Vec::new();
            let mut stack = vec![node.id];

            while let Some(current) = stack.pop() {
                if visited[current] {
                    continue;
                }

                visited[current] = true;
                component.push(current);

                for &neighbor in &self.nodes[current].connected_to {
                    if !visited[neighbor] {
                        stack.push(neighbor);
                    }
                }
            }

            if !component.is_empty() {
                components.push(component);
            }
        }

        components
    }

    /// Generate ASCII visualization of the connectivity graph
    pub fn render_ascii(&self, max_nodes: usize) -> String {
        let mut output = String::new();

        output.push_str(&format!(
            "Connectivity Graph ({} nodes, {} edges)\n",
            self.nodes.len(),
            self.edges.len()
        ));
        output.push_str(&format!("{}\n", "=".repeat(50)));

        // Show top connected nodes
        let hubs = self.get_hubs(max_nodes.min(10));
        output.push_str("\nTop Connected Nodes (Hubs):\n");
        for node in &hubs {
            output.push_str(&format!(
                "  Node {:3}: degree={:3}, connected to: {:?}\n",
                node.id,
                node.degree,
                &node.connected_to[..node.connected_to.len().min(5)]
            ));
        }

        // Show connected components
        let components = self.connected_components();
        output.push_str(&format!("\nConnected Components: {}\n", components.len()));
        for (i, comp) in components.iter().take(5).enumerate() {
            output.push_str(&format!(
                "  Component {}: {} nodes - {:?}\n",
                i,
                comp.len(),
                &comp[..comp.len().min(10)]
            ));
        }

        // Show edge statistics
        let total_weight: f64 = self.edges.iter().map(|e| e.weight).sum();
        let avg_weight = if !self.edges.is_empty() {
            total_weight / self.edges.len() as f64
        } else {
            0.0
        };

        output.push_str(&format!("\nEdge Statistics:\n"));
        output.push_str(&format!("  Total edges: {}\n", self.edges.len()));
        output.push_str(&format!("  Avg shared columns: {:.2}\n", avg_weight));

        output
    }

    /// Export graph to DOT format for Graphviz visualization
    pub fn to_dot(&self, max_nodes: usize) -> String {
        let mut dot = String::from("graph G {\n");
        dot.push_str("  layout=neato;\n");
        dot.push_str("  node [shape=circle];\n");

        // Only show top connected nodes to avoid clutter
        let hubs = self.get_hubs(max_nodes);
        let hub_ids: HashSet<usize> = hubs.iter().map(|n| n.id).collect();

        // Add nodes
        for node in &hubs {
            let size = 0.3 + (node.degree as f64 * 0.1);
            dot.push_str(&format!(
                "  {} [label=\"{}\", width={}];\n",
                node.id, node.id, size
            ));
        }

        // Add edges between hub nodes only
        for edge in &self.edges {
            if hub_ids.contains(&edge.from) && hub_ids.contains(&edge.to) {
                let width = 1.0 + edge.weight * 0.5;
                dot.push_str(&format!(
                    "  {} -- {} [penwidth={}];\n",
                    edge.from, edge.to, width
                ));
            }
        }

        dot.push_str("}\n");
        dot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_connectivity_graph() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row", DataType::UInt32, false),
            Field::new("col", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let row = Arc::new(UInt32Array::from(vec![0, 0, 1, 1, 2])) as ArrayRef;
        let col = Arc::new(UInt32Array::from(vec![0, 1, 1, 2, 2])) as ArrayRef;
        let val = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])) as ArrayRef;

        let batch = RecordBatch::try_new(schema, vec![row, col, val]).unwrap();
        let graph = ConnectivityGraph::from_coo_batch(&batch).unwrap();

        assert_eq!(graph.nodes.len(), 3);
        assert!(!graph.edges.is_empty());

        let hubs = graph.get_hubs(3);
        assert!(!hubs.is_empty());
    }
}
