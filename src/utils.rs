use std::fmt::Write;

pub fn print_ascii_table<T>(headers: &[&str], rows: &[T]) -> String
where
    T: AsRef<[String]>,
{
    if headers.is_empty() {
        return String::new();
    }

    let col_count = headers.len();
    let mut col_widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

    for row in rows {
        let cells = row.as_ref();
        for (i, cell) in cells.iter().enumerate().take(col_count) {
            col_widths[i] = col_widths[i].max(cell.len());
        }
    }

    let mut output = String::new();

    write!(output, "┌").unwrap();
    for (i, w) in col_widths.iter().enumerate() {
        output.push_str(&"─".repeat(*w + 2));
        if i + 1 < col_count {
            output.push('┬');
        }
    }
    writeln!(output, "┐").unwrap();

    for (i, header) in headers.iter().enumerate() {
        write!(output, "│ {:width$} ", header, width = col_widths[i]).unwrap();
    }
    writeln!(output, "│").unwrap();

    write!(output, "├").unwrap();
    for (i, w) in col_widths.iter().enumerate() {
        output.push_str(&"─".repeat(*w + 2));
        if i + 1 < col_count {
            output.push('┼');
        }
    }
    writeln!(output, "┤").unwrap();

    for row in rows {
        let cells = row.as_ref();
        for (i, cell) in cells.iter().enumerate().take(col_count) {
            write!(output, "│ {:width$} ", cell, width = col_widths[i]).unwrap();
        }
        writeln!(output, "│").unwrap();
    }

    write!(output, "└").unwrap();
    for (i, w) in col_widths.iter().enumerate() {
        output.push_str(&"─".repeat(*w + 2));
        if i + 1 < col_count {
            output.push('┴');
        }
    }
    writeln!(output, "┘").unwrap();

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_table() {
        let headers: Vec<&str> = vec![];
        let rows: Vec<Vec<String>> = vec![];
        let result = print_ascii_table(&headers, &rows);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_column() {
        let headers = vec!["Name"];
        let rows = vec![vec!["Alice".to_string()], vec!["Bob".to_string()]];
        let result = print_ascii_table(&headers, &rows);
        assert!(result.contains("Name"));
        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
    }

    #[test]
    fn test_multiple_columns() {
        let headers = vec!["ID", "Name", "Amount"];
        let rows = vec![
            vec!["1".to_string(), "Alice".to_string(), "100".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "200".to_string()],
        ];
        let result = print_ascii_table(&headers, &rows);
        assert!(result.contains("ID"));
        assert!(result.contains("Name"));
        assert!(result.contains("Amount"));
    }

    #[test]
    fn test_column_width_sizing() {
        let headers = vec!["Short", "LongerColumn"];
        let rows = vec![vec!["A".to_string(), "This is longer".to_string()]];
        let result = print_ascii_table(&headers, &rows);
        assert!(result.contains("LongerColumn"));
        assert!(result.contains("This is longer"));
    }

    #[test]
    fn test_separators() {
        let headers = vec!["A", "B", "C"];
        let rows = vec![vec!["1".to_string(), "2".to_string(), "3".to_string()]];
        let result = print_ascii_table(&headers, &rows);
        assert!(result.contains('┬'));
        assert!(result.contains('┼'));
        assert!(result.contains('┴'));
    }
}
