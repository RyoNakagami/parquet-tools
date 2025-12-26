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

```text
=== File Info ===
Path: data.parquet
Rows: 1,000
Columns: 3
Row Groups: 1
Compression: SNAPPY
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
  compression: SNAPPY
  created_by: parquet-cpp ...
schema:
  id: int64
  name: string
  value: double
```

### merge

Merge multiple Parquet files into a single file.

```bash
# Merge all .parquet files in a directory (default: snappy compression)
parquet-tools merge /path/to/input_dir

# Specify output file
parquet-tools merge /path/to/input_dir -o merged.parquet

# Specify compression codec
parquet-tools merge /path/to/input_dir -c zstd
parquet-tools merge /path/to/input_dir -c gzip
parquet-tools merge /path/to/input_dir -c lz4
parquet-tools merge /path/to/input_dir -c none
```

**Options:**

- `-o, --output`: Output file path (default: `<input_dir>_merged.parquet`)
- `-c, --compression`: Compression codec (default: `snappy`)

**Compression codecs:**

| Codec | Description |
|-------|-------------|
| `snappy` | Standard, balanced speed/compression (default) |
| `zstd` | High compression ratio, recommended for storage |
| `gzip` | High compression, good for archiving |
| `lz4` | Fast compression, good for streaming |
| `none` | No compression |

### csv2parquet

Convert a CSV file to Parquet format.

```bash
# Basic conversion (all columns as string, snappy compression)
parquet-tools csv2parquet data.csv

# Specify output file and compression
parquet-tools csv2parquet data.csv -o data.parquet -c zstd

# With schema file for explicit typing
parquet-tools csv2parquet data.csv --schema schema.yaml
parquet-tools csv2parquet data.csv --schema schema.json -c zstd
```

**Options:**

- `-o, --output`: Output Parquet file path (default: `<input_basename>.parquet`)
- `-c, --compression`: Compression codec (default: `snappy`)
- `--schema`: Schema file (.yaml or .json) for column typing

**Default behavior:**

- Without `--schema`, all columns are written as `string` type
- With `--schema`, specified columns are cast to their defined types
- Columns not in schema remain `string`

**Schema format (YAML):**

```yaml
fields:
  - name: id
    type: int64
  - name: created_at
    type: timestamp
  - name: value
    type: float64
```

**Schema format (JSON):**

```json
{
  "fields": [
    { "name": "id", "type": "int64" },
    { "name": "created_at", "type": "timestamp" },
    { "name": "value", "type": "float64" }
  ]
}
```

**Supported types:**

| Type | Description |
|------|-------------|
| `string` | Text data |
| `int64` | 64-bit integer |
| `float64` | 64-bit floating point |
| `boolean` | True/False |
| `timestamp` | Date and time (microsecond precision) |
| `date` | Date only |

**Unsupported types:**

- `array<T>` / `list<T>` - Array/List types
- `struct<...>` - Nested struct types
- `map<K,V>` - Map types

For detailed schema documentation, see [docs/schema-guide.md](docs/guidelines/schema-guide.md).

## Requirements

- Python >= 3.13

## License

MIT
