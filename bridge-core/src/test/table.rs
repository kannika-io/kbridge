//! Module for formatting tabular data with automatic column sizing.

use std::fmt::Display;

/// A builder for creating formatted tables with aligned columns.
pub struct TableBuilder {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl TableBuilder {
    /// Creates a new table builder with the specified headers.
    pub fn new(headers: Vec<String>) -> Self {
        TableBuilder {
            headers,
            rows: Vec::new(),
        }
    }

    /// Adds a row to the table.
    pub fn add_row(&mut self, row: Vec<String>) {
        assert_eq!(
            row.len(),
            self.headers.len(),
            "Row length must match header length"
        );
        self.rows.push(row);
    }

    /// Builds the table as a formatted string.
    pub fn build(&self) -> String {
        if self.headers.is_empty() {
            return String::new();
        }

        // Calculate column widths
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.len()).collect();

        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.len());
            }
        }

        let mut output = String::new();

        // Header row
        output.push('|');
        for (i, header) in self.headers.iter().enumerate() {
            output.push_str(&format!(" {:<width$} |", header, width = widths[i]));
        }
        output.push('\n');

        // Separator
        output.push('|');
        for &width in &widths {
            output.push_str(&format!("{:-<width$}|", "", width = width + 2));
        }
        output.push('\n');

        // Data rows
        for row in &self.rows {
            output.push('|');
            for (i, cell) in row.iter().enumerate() {
                output.push_str(&format!(" {:<width$} |", cell, width = widths[i]));
            }
            output.push('\n');
        }

        output
    }
}

/// Extension trait for converting values to table cells.
pub trait ToCell {
    fn to_cell(&self) -> String;
}

impl<T: Display> ToCell for T {
    fn to_cell(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_table() {
        let table = TableBuilder::new(vec![]);
        assert_eq!(table.build(), "");
    }

    #[test]
    fn test_table_with_headers_only() {
        let table = TableBuilder::new(vec!["Name".to_string(), "Age".to_string()]);
        let output = table.build();
        assert!(output.contains("Name"));
        assert!(output.contains("Age"));
    }

    #[test]
    fn test_table_with_data() {
        let mut table = TableBuilder::new(vec![
            "Name".to_string(),
            "Age".to_string(),
            "City".to_string(),
        ]);
        table.add_row(vec![
            "Alice".to_string(),
            "30".to_string(),
            "NYC".to_string(),
        ]);
        table.add_row(vec!["Bob".to_string(), "25".to_string(), "LA".to_string()]);

        let output = table.build();
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
        assert!(output.contains("30"));
        assert!(output.contains("25"));
    }

    #[test]
    fn test_column_alignment() {
        let mut table = TableBuilder::new(vec!["Short".to_string(), "VeryLongHeader".to_string()]);
        table.add_row(vec!["A".to_string(), "ShortValue".to_string()]);

        let output = table.build();
        // The second column should be sized to fit "VeryLongHeader"
        assert!(output.contains("VeryLongHeader"));
    }

    #[test]
    #[should_panic(expected = "Row length must match header length")]
    fn test_mismatched_row_length() {
        let mut table = TableBuilder::new(vec!["A".to_string(), "B".to_string()]);
        table.add_row(vec!["1".to_string()]); // Wrong length
    }
}
