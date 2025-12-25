# parquet-tools

CLI tools for working with Parquet files.

## Installation

```bash
# from the current directory
uv tool install .

# from GitHub
uv tool install https://github.com/RyoNakagami/parquet-tools.git
```

Or run directly with `uvx`:

```bash
uvx --from . parquet-tools <command>
```

## Commands

### head

Display the first N rows of a Parquet file.

```bash
# Display first 10 rows (default)
parquet-tools head data.parquet

# Display first 20 rows
parquet-tools head data.parquet -n 20

# Export to CSV
parquet-tools head data.parquet -n 100 -o output.csv
```

**Options:**

- `-n, --rows`: Number of rows to display (default: 10)
- `-o, --output`: Output to CSV file instead of stdout

### info

Display metadata and schema information of a Parquet file.

```bash
# Display info
parquet-tools info data.parquet

# Output in YAML format
parquet-tools info data.parquet --yaml
```

**Options:**

- `--yaml`: Output in YAML format

**Example output:**

```bash
=== File Info ===
Path: data.parquet
Rows: 1,000
Columns: 3
Row Groups: 1
Created By: parquet-cpp ...

=== Schema ===
  id: int64
  name: string
  value: double
```

**YAML output:**

```yaml
file:
  path: data.parquet
  rows: 1000
  columns: 3
  row_groups: 1
  created_by: parquet-cpp ...
schema:
  id: int64
  name: string
  value: double
```

### merge

Merge multiple Parquet files into a single file.

```bash
# Merge all .parquet files in a directory
parquet-tools merge /path/to/input_dir

# Specify output file
parquet-tools merge /path/to/input_dir -o merged.parquet
```

**Options:**

- `-o, --output`: Output file path (default: `<input_dir>_merged.parquet`)

## Requirements

- Python >= 3.13

## License

MIT
