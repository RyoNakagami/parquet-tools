from pathlib import Path
from typing import Annotated, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import typer

app = typer.Typer(help="CLI tools for working with Parquet files")


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


@app.command()
def info(
    file: Annotated[Path, typer.Argument(help="Parquet file to inspect")],
    yaml_output: Annotated[
        bool, typer.Option("--yaml", help="Output in YAML format")
    ] = False,
) -> None:
    """Display metadata and schema information of a Parquet file."""
    if not file.exists():
        typer.echo(f"File not found: {file}")
        raise typer.Exit(1)

    parquet_file = pq.ParquetFile(file)
    metadata = parquet_file.metadata
    schema = parquet_file.schema_arrow

    if yaml_output:
        import yaml

        data = {
            "file": {
                "path": str(file),
                "rows": metadata.num_rows,
                "columns": metadata.num_columns,
                "row_groups": metadata.num_row_groups,
                "created_by": metadata.created_by,
            },
            "schema": {field.name: str(field.type) for field in schema},
        }
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
    pq.write_table(merged_table, output_path)
    typer.echo(f"Saved: {output_path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
