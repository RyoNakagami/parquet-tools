import json
import sys
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import typer

from parquet_tools.library import __version__


class Compression(str, Enum):
    """Parquet compression codecs."""

    NONE = "none"
    SNAPPY = "snappy"
    ZSTD = "zstd"
    GZIP = "gzip"
    LZ4 = "lz4"


app = typer.Typer(help="CLI tools for working with Parquet files")


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"parquet-tools {__version__}")
        typer.echo(f"Python {sys.version.split()[0]}")
        raise typer.Exit()


@app.callback()
def callback(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            callback=version_callback,
            is_eager=True,
            help="Show version and exit",
        ),
    ] = False,
) -> None:
    """CLI tools for working with Parquet files."""
    pass


@app.command()
def head(
    file: Annotated[Path, typer.Argument(help="Parquet file to read")],
    rows: Annotated[
        int, typer.Option("-n", "--rows", help="Number of rows to display")
    ] = 10,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Output to CSV file instead of stdout"),
    ] = None,
) -> None:
    """Display the first N rows of a Parquet file."""
    if not file.exists():
        typer.echo(f"File not found: {file}")
        raise typer.Exit(1)

    table = pq.read_table(file)
    df = table.slice(0, rows).to_pandas()

    if output:
        df.to_csv(output, index=False)
        typer.echo(f"Saved: {output}")
    else:
        typer.echo(df.to_string())


def _get_compression_info(parquet_file: pq.ParquetFile) -> str:
    """Get compression codec from parquet file."""
    if parquet_file.metadata.num_row_groups == 0:
        return "unknown"
    row_group = parquet_file.metadata.row_group(0)
    if row_group.num_columns == 0:
        return "unknown"
    return row_group.column(0).compression


@app.command()
def info(
    file: Annotated[Path, typer.Argument(help="Parquet file to inspect")],
    yaml_output: Annotated[
        bool, typer.Option("--yaml", help="Output in YAML format")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Output in JSON format")
    ] = False,
) -> None:
    """Display metadata and schema information of a Parquet file."""
    if not file.exists():
        typer.echo(f"File not found: {file}")
        raise typer.Exit(1)

    if yaml_output and json_output:
        typer.echo("Error: Cannot specify both --yaml and --json.")
        raise typer.Exit(1)

    parquet_file = pq.ParquetFile(file)
    metadata = parquet_file.metadata
    schema = parquet_file.schema_arrow
    compression = _get_compression_info(parquet_file)

    if yaml_output or json_output:
        # Build fields array preserving column order
        fields = [{"name": field.name, "type": str(field.type)} for field in schema]

        data = {
            "file": {
                "path": str(file),
                "rows": metadata.num_rows,
                "columns": metadata.num_columns,
                "row_groups": metadata.num_row_groups,
                "compression": compression,
                "created_by": metadata.created_by,
            },
            "fields": fields,
        }

        if json_output:
            typer.echo(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            import yaml

            typer.echo(
                yaml.dump(
                    data, default_flow_style=False, allow_unicode=True, sort_keys=False
                )
            )
    else:
        typer.echo("=== File Info ===")
        typer.echo(f"Path: {file}")
        typer.echo(f"Rows: {metadata.num_rows:,}")
        typer.echo(f"Columns: {metadata.num_columns}")
        typer.echo(f"Row Groups: {metadata.num_row_groups}")
        typer.echo(f"Compression: {compression}")
        typer.echo(f"Created By: {metadata.created_by}")

        typer.echo("\n=== Schema ===")
        for field in schema:
            typer.echo(f"  {field.name}: {field.type}")


@app.command()
def merge(
    input_dir: Annotated[
        Path, typer.Argument(help="Directory containing .parquet files to merge")
    ],
    output: Annotated[
        Optional[Path],
        typer.Option(
            "-o",
            "--output",
            help="Output file path (default: <input_dir>_merged.parquet)",
        ),
    ] = None,
    compression: Annotated[
        Compression,
        typer.Option(
            "-c",
            "--compression",
            help="Compression codec: none, snappy, zstd, gzip, lz4",
        ),
    ] = Compression.SNAPPY,
) -> None:
    """Merge multiple Parquet files into a single file."""
    parquet_files = sorted(input_dir.glob("*.parquet"))

    if not parquet_files:
        typer.echo(f"No .parquet files found in {input_dir}")
        raise typer.Exit(1)

    typer.echo(f"Files found: {len(parquet_files)}")

    tables = [pq.read_table(f) for f in parquet_files]
    merged_table = pa.concat_tables(tables)

    typer.echo(
        f"Merged: {merged_table.num_rows:,} rows, {merged_table.num_columns} columns"
    )

    output_path = (
        output if output else input_dir.parent / f"{input_dir.name}_merged.parquet"
    )

    codec = None if compression == Compression.NONE else compression.value
    pq.write_table(merged_table, output_path, compression=codec)
    typer.echo(f"Saved: {output_path} (compression: {compression.value})")


# Type mapping from schema type names to PyArrow types
_TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "int64": pa.int64(),
    "float64": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
}


def _load_schema(schema_path: Path) -> dict[str, pa.DataType]:
    """Load schema from YAML or JSON file and return column->type mapping."""
    import yaml

    content = schema_path.read_text()
    suffix = schema_path.suffix.lower()

    if suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(content)
    elif suffix == ".json":
        data = json.loads(content)
    else:
        raise typer.BadParameter(
            f"Unsupported schema format: {suffix}. Use .yaml, .yml, or .json"
        )

    if "fields" not in data:
        raise typer.BadParameter("Schema must contain 'fields' key")

    type_mapping: dict[str, pa.DataType] = {}
    for field in data["fields"]:
        name = field.get("name")
        type_str = field.get("type", "string")

        if not name:
            raise typer.BadParameter("Each field must have a 'name'")

        if type_str not in _TYPE_MAP:
            supported = ", ".join(_TYPE_MAP.keys())
            raise typer.BadParameter(
                f"Unknown type '{type_str}' for column '{name}'. "
                f"Supported types: {supported}"
            )

        type_mapping[name] = _TYPE_MAP[type_str]

    return type_mapping


def _cast_table_with_schema(
    table: pa.Table, type_mapping: dict[str, pa.DataType]
) -> pa.Table:
    """Cast table columns according to schema. Columns not in schema remain string."""
    csv_columns = set(table.column_names)
    schema_columns = set(type_mapping.keys())

    # Check for schema columns not in CSV
    missing_in_csv = schema_columns - csv_columns
    if missing_in_csv:
        raise typer.BadParameter(
            f"Schema defines columns not found in CSV: {', '.join(sorted(missing_in_csv))}"
        )

    # Build new schema and cast columns
    new_fields = []
    for col_name in table.column_names:
        if col_name in type_mapping:
            new_fields.append(pa.field(col_name, type_mapping[col_name]))
        else:
            new_fields.append(pa.field(col_name, pa.string()))

    target_schema = pa.schema(new_fields)
    return table.cast(target_schema)


@app.command()
def csv2parquet(
    file: Annotated[Path, typer.Argument(help="CSV file to convert")],
    output: Annotated[
        Optional[Path],
        typer.Option(
            "-o",
            "--output",
            help="Output Parquet file path (default: <input_basename>.parquet)",
        ),
    ] = None,
    compression: Annotated[
        Compression,
        typer.Option(
            "-c",
            "--compression",
            help="Compression codec: none, snappy, zstd, gzip, lz4",
        ),
    ] = Compression.SNAPPY,
    schema: Annotated[
        Optional[Path],
        typer.Option(
            "--schema",
            help="Schema file (.yaml or .json) for column typing. "
            "Without schema, all columns are written as string.",
        ),
    ] = None,
) -> None:
    """Convert a CSV file to Parquet format.

    By default, all columns are written as string type.
    Use --schema to specify explicit column types via YAML or JSON file.

    Schema format example (YAML):

        fields:

          - name: id

            type: int64

          - name: created_at

            type: timestamp

    Supported types: string, int64, float64, boolean, timestamp, date
    """
    if not file.exists():
        typer.echo(f"File not found: {file}")
        raise typer.Exit(1)

    if not file.suffix.lower() == ".csv":
        typer.echo(f"Warning: Input file does not have .csv extension: {file}")

    # Load schema if provided
    type_mapping: dict[str, pa.DataType] | None = None
    if schema:
        if not schema.exists():
            typer.echo(f"Schema file not found: {schema}")
            raise typer.Exit(1)
        type_mapping = _load_schema(schema)
        typer.echo(f"Schema loaded: {len(type_mapping)} column type(s) defined")

    # Read CSV with all columns as string (PyArrow streaming read)
    # Using read_csv with column_types to force all strings
    typer.echo(f"Reading: {file}")
    read_options = pa_csv.ReadOptions()
    convert_options = pa_csv.ConvertOptions(strings_can_be_null=True)

    # Read all as string first for consistent behavior
    table = pa_csv.read_csv(
        file,
        read_options=read_options,
        convert_options=convert_options,
    )

    # Cast all columns to string first (ensures default behavior)
    string_schema = pa.schema(
        [pa.field(name, pa.string()) for name in table.column_names]
    )
    table = table.cast(string_schema)

    # Apply schema typing if provided
    if type_mapping:
        table = _cast_table_with_schema(table, type_mapping)

    typer.echo(f"Rows: {table.num_rows:,}, Columns: {table.num_columns}")

    # Determine output path
    output_path = output if output else file.with_suffix(".parquet")

    # Write Parquet
    codec = None if compression == Compression.NONE else compression.value
    pq.write_table(table, output_path, compression=codec)
    typer.echo(f"Saved: {output_path} (compression: {compression.value})")


@app.command()
def query(
    file: Annotated[Path, typer.Argument(help="Parquet file to query")],
    sql: Annotated[
        Optional[str],
        typer.Argument(help="SQL query to execute (use 'data' as table name)"),
    ] = None,
    sql_file: Annotated[
        Optional[Path],
        typer.Option("--sql-file", help="Path to SQL file containing the query"),
    ] = None,
    output: Annotated[
        Optional[Path],
        typer.Option("-o", "--output", help="Output file path (CSV format)"),
    ] = None,
) -> None:
    """Execute SQL queries on a Parquet file using DuckDB.

    The Parquet file is exposed as a table named 'data'.

    Examples:

        parquet-tools query data.parquet "SELECT * FROM data LIMIT 10"

        parquet-tools query data.parquet --sql-file analysis.sql -o result.csv
    """
    import duckdb

    # Validate file exists
    if not file.exists():
        typer.echo(f"File not found: {file}")
        raise typer.Exit(1)

    # Validate SQL input: exactly one of (sql, sql_file) must be provided
    if sql is not None and sql_file is not None:
        typer.echo(
            "Error: Cannot specify both SQL string and --sql-file. Use one or the other."
        )
        raise typer.Exit(1)

    if sql is None and sql_file is None:
        typer.echo("Error: Must provide either SQL string or --sql-file.")
        raise typer.Exit(1)

    # Read SQL from file if specified
    query_sql: str = sql if sql is not None else ""
    if sql_file is not None:
        if not sql_file.exists():
            typer.echo(f"SQL file not found: {sql_file}")
            raise typer.Exit(1)
        try:
            query_sql = sql_file.read_text().strip()
        except OSError as e:
            typer.echo(f"Error reading SQL file: {e}")
            raise typer.Exit(1)

        if not query_sql:
            typer.echo(f"SQL file is empty: {sql_file}")
            raise typer.Exit(1)

    # Execute query with DuckDB
    try:
        con = duckdb.connect(":memory:")
        # Create a view named 'data' pointing to the parquet file
        # Escape single quotes in file path to handle paths with special characters
        escaped_path = str(file).replace("'", "''")
        con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{escaped_path}')")
        result = con.execute(query_sql).df()
    except duckdb.Error as e:
        typer.echo(f"SQL error: {e}")
        raise typer.Exit(1)
    finally:
        # Close DuckDB connection ensuring resources are released
        con.close()

    # Output result
    if output:
        try:
            result.to_csv(output, index=False)
            typer.echo(f"Saved: {output}")
        except OSError as e:
            typer.echo(f"Error writing output file: {e}")
            raise typer.Exit(1)
    else:
        typer.echo(result.to_string())


def main() -> None:
    app()


if __name__ == "__main__":
    main()
