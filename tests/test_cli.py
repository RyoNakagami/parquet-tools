"""Tests for parquet-tools CLI commands."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from typer.testing import CliRunner

from parquet_tools.cli import (
    Compression,
    _cast_table_with_schema,
    _get_compression_info,
    _load_schema,
    app,
)

runner = CliRunner()


class TestHeadCommand:
    """Tests for the 'head' command."""

    def test_head_default_rows(self, tmp_parquet_file: Path) -> None:
        """Test head command with default number of rows."""
        result = runner.invoke(app, ["head", str(tmp_parquet_file)])
        assert result.exit_code == 0
        # Should display all 5 rows (default is 10, file has 5)
        assert "Alice" in result.stdout
        assert "Eve" in result.stdout

    def test_head_limited_rows(self, large_parquet_file: Path) -> None:
        """Test head command with limited number of rows."""
        result = runner.invoke(app, ["head", str(large_parquet_file), "-n", "5"])
        assert result.exit_code == 0
        # Should only show first 5 rows
        assert "value_0" in result.stdout
        assert "value_4" in result.stdout
        # Should not show row 10 or beyond
        assert "value_10" not in result.stdout

    def test_head_output_to_csv(self, tmp_parquet_file: Path, tmp_path: Path) -> None:
        """Test head command with CSV output."""
        output_csv = tmp_path / "output.csv"
        result = runner.invoke(
            app, ["head", str(tmp_parquet_file), "-o", str(output_csv)]
        )
        assert result.exit_code == 0
        assert output_csv.exists()
        assert "Saved:" in result.stdout

        # Verify CSV content
        df = pd.read_csv(output_csv)
        assert len(df) == 5
        assert list(df.columns) == ["id", "name", "value"]

    def test_head_file_not_found(self, tmp_path: Path) -> None:
        """Test head command with non-existent file."""
        result = runner.invoke(app, ["head", str(tmp_path / "nonexistent.parquet")])
        assert result.exit_code == 1
        assert "File not found" in result.stdout

    def test_head_with_rows_option_long_form(self, large_parquet_file: Path) -> None:
        """Test head command with --rows option."""
        result = runner.invoke(app, ["head", str(large_parquet_file), "--rows", "3"])
        assert result.exit_code == 0
        assert "value_0" in result.stdout
        assert "value_2" in result.stdout
        assert "value_3" not in result.stdout


class TestInfoCommand:
    """Tests for the 'info' command."""

    def test_info_basic(self, tmp_parquet_file: Path) -> None:
        """Test info command with basic output."""
        result = runner.invoke(app, ["info", str(tmp_parquet_file)])
        assert result.exit_code == 0
        assert "=== File Info ===" in result.stdout
        assert "Rows:" in result.stdout
        assert "Columns:" in result.stdout
        assert "=== Schema ===" in result.stdout
        assert "id:" in result.stdout
        assert "name:" in result.stdout

    def test_info_yaml_output(self, tmp_parquet_file: Path) -> None:
        """Test info command with YAML output."""
        result = runner.invoke(app, ["info", str(tmp_parquet_file), "--yaml"])
        assert result.exit_code == 0

        # Parse YAML output
        data = yaml.safe_load(result.stdout)
        assert "file" in data
        assert "schema" in data
        assert data["file"]["rows"] == 5
        assert data["file"]["columns"] == 3
        assert "id" in data["schema"]
        assert "name" in data["schema"]

    def test_info_shows_compression(self, tmp_parquet_file_gzip: Path) -> None:
        """Test info command shows compression codec."""
        result = runner.invoke(app, ["info", str(tmp_parquet_file_gzip)])
        assert result.exit_code == 0
        assert "Compression:" in result.stdout
        assert "GZIP" in result.stdout

    def test_info_file_not_found(self, tmp_path: Path) -> None:
        """Test info command with non-existent file."""
        result = runner.invoke(app, ["info", str(tmp_path / "nonexistent.parquet")])
        assert result.exit_code == 1
        assert "File not found" in result.stdout


class TestMergeCommand:
    """Tests for the 'merge' command."""

    def test_merge_basic(self, tmp_parquet_dir: Path, tmp_path: Path) -> None:
        """Test basic merge functionality."""
        output_file = tmp_path / "merged.parquet"
        result = runner.invoke(
            app, ["merge", str(tmp_parquet_dir), "-o", str(output_file)]
        )
        assert result.exit_code == 0
        assert output_file.exists()
        assert "Files found: 3" in result.stdout
        assert "Merged:" in result.stdout

        # Verify merged content
        df = pd.read_parquet(output_file)
        assert len(df) == 9  # 3 files * 3 rows each

    def test_merge_default_output_path(self, tmp_parquet_dir: Path) -> None:
        """Test merge with default output path."""
        result = runner.invoke(app, ["merge", str(tmp_parquet_dir)])
        assert result.exit_code == 0

        # Check default output path
        expected_output = tmp_parquet_dir.parent / f"{tmp_parquet_dir.name}_merged.parquet"
        assert expected_output.exists()

    def test_merge_with_compression(self, tmp_parquet_dir: Path, tmp_path: Path) -> None:
        """Test merge with different compression codecs."""
        for codec in ["snappy", "zstd", "gzip", "lz4", "none"]:
            output_file = tmp_path / f"merged_{codec}.parquet"
            result = runner.invoke(
                app,
                ["merge", str(tmp_parquet_dir), "-o", str(output_file), "-c", codec],
            )
            assert result.exit_code == 0
            assert output_file.exists()
            assert f"compression: {codec}" in result.stdout

    def test_merge_empty_directory(self, tmp_empty_parquet_dir: Path) -> None:
        """Test merge with empty directory."""
        result = runner.invoke(app, ["merge", str(tmp_empty_parquet_dir)])
        assert result.exit_code == 1
        assert "No .parquet files found" in result.stdout


class TestCsv2ParquetCommand:
    """Tests for the 'csv2parquet' command."""

    def test_csv2parquet_basic(self, tmp_csv_file: Path, tmp_path: Path) -> None:
        """Test basic CSV to Parquet conversion."""
        output_file = tmp_path / "output.parquet"
        result = runner.invoke(
            app, ["csv2parquet", str(tmp_csv_file), "-o", str(output_file)]
        )
        assert result.exit_code == 0
        assert output_file.exists()
        assert "Saved:" in result.stdout

        # Verify content
        df = pd.read_parquet(output_file)
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "value"]

    def test_csv2parquet_default_output(self, tmp_csv_file: Path) -> None:
        """Test CSV to Parquet with default output path."""
        result = runner.invoke(app, ["csv2parquet", str(tmp_csv_file)])
        assert result.exit_code == 0

        expected_output = tmp_csv_file.with_suffix(".parquet")
        assert expected_output.exists()

    def test_csv2parquet_with_yaml_schema(
        self, tmp_csv_file: Path, tmp_yaml_schema: Path, tmp_path: Path
    ) -> None:
        """Test CSV to Parquet with YAML schema."""
        # Create CSV matching the schema
        csv_path = tmp_path / "typed.csv"
        csv_path.write_text("id,name,amount,active\n1,Alice,100.5,true\n2,Bob,200.3,false\n")

        output_file = tmp_path / "output.parquet"
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(csv_path),
                "-o",
                str(output_file),
                "--schema",
                str(tmp_yaml_schema),
            ],
        )
        assert result.exit_code == 0
        assert "Schema loaded:" in result.stdout

        # Verify types
        table = pq.read_table(output_file)
        assert table.schema.field("id").type == pa.int64()
        assert table.schema.field("name").type == pa.string()
        assert table.schema.field("amount").type == pa.float64()
        assert table.schema.field("active").type == pa.bool_()

    def test_csv2parquet_with_json_schema(
        self, tmp_csv_file: Path, tmp_json_schema: Path, tmp_path: Path
    ) -> None:
        """Test CSV to Parquet with JSON schema."""
        output_file = tmp_path / "output.parquet"
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(tmp_csv_file),
                "-o",
                str(output_file),
                "--schema",
                str(tmp_json_schema),
            ],
        )
        assert result.exit_code == 0
        assert "Schema loaded:" in result.stdout

        # Verify types
        table = pq.read_table(output_file)
        assert table.schema.field("id").type == pa.int64()
        assert table.schema.field("value").type == pa.float64()

    def test_csv2parquet_all_string_without_schema(
        self, tmp_csv_file: Path, tmp_path: Path
    ) -> None:
        """Test that all columns are string type without schema."""
        output_file = tmp_path / "output.parquet"
        result = runner.invoke(
            app, ["csv2parquet", str(tmp_csv_file), "-o", str(output_file)]
        )
        assert result.exit_code == 0

        # Verify all columns are string
        table = pq.read_table(output_file)
        for field in table.schema:
            assert field.type == pa.string()

    def test_csv2parquet_with_compression(
        self, tmp_csv_file: Path, tmp_path: Path
    ) -> None:
        """Test CSV to Parquet with different compression codecs."""
        for codec in ["snappy", "zstd", "gzip", "lz4", "none"]:
            output_file = tmp_path / f"output_{codec}.parquet"
            result = runner.invoke(
                app,
                ["csv2parquet", str(tmp_csv_file), "-o", str(output_file), "-c", codec],
            )
            assert result.exit_code == 0
            assert output_file.exists()
            assert f"compression: {codec}" in result.stdout

    def test_csv2parquet_file_not_found(self, tmp_path: Path) -> None:
        """Test csv2parquet with non-existent file."""
        result = runner.invoke(app, ["csv2parquet", str(tmp_path / "nonexistent.csv")])
        assert result.exit_code == 1
        assert "File not found" in result.stdout

    def test_csv2parquet_schema_not_found(
        self, tmp_csv_file: Path, tmp_path: Path
    ) -> None:
        """Test csv2parquet with non-existent schema file."""
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(tmp_csv_file),
                "--schema",
                str(tmp_path / "nonexistent.yaml"),
            ],
        )
        assert result.exit_code == 1
        assert "Schema file not found" in result.stdout

    def test_csv2parquet_warns_non_csv_extension(self, tmp_path: Path) -> None:
        """Test csv2parquet warns about non-.csv extension."""
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("a,b\n1,2\n")

        result = runner.invoke(app, ["csv2parquet", str(txt_file)])
        assert result.exit_code == 0
        assert "Warning: Input file does not have .csv extension" in result.stdout


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_get_compression_info_snappy(self, tmp_parquet_file: Path) -> None:
        """Test _get_compression_info with snappy compression."""
        parquet_file = pq.ParquetFile(tmp_parquet_file)
        compression = _get_compression_info(parquet_file)
        assert compression == "SNAPPY"

    def test_get_compression_info_gzip(self, tmp_parquet_file_gzip: Path) -> None:
        """Test _get_compression_info with gzip compression."""
        parquet_file = pq.ParquetFile(tmp_parquet_file_gzip)
        compression = _get_compression_info(parquet_file)
        assert compression == "GZIP"

    def test_get_compression_info_none(
        self, tmp_parquet_file_no_compression: Path
    ) -> None:
        """Test _get_compression_info with no compression."""
        parquet_file = pq.ParquetFile(tmp_parquet_file_no_compression)
        compression = _get_compression_info(parquet_file)
        assert compression == "UNCOMPRESSED"

    def test_load_schema_yaml(self, tmp_yaml_schema: Path) -> None:
        """Test _load_schema with YAML file."""
        type_mapping = _load_schema(tmp_yaml_schema)
        assert "id" in type_mapping
        assert type_mapping["id"] == pa.int64()
        assert type_mapping["name"] == pa.string()
        assert type_mapping["amount"] == pa.float64()
        assert type_mapping["active"] == pa.bool_()

    def test_load_schema_json(self, tmp_json_schema: Path) -> None:
        """Test _load_schema with JSON file."""
        type_mapping = _load_schema(tmp_json_schema)
        assert "id" in type_mapping
        assert type_mapping["id"] == pa.int64()
        assert type_mapping["value"] == pa.float64()

    def test_load_schema_invalid_format(self, tmp_path: Path, tmp_csv_file: Path) -> None:
        """Test _load_schema with unsupported format."""
        invalid_schema = tmp_path / "schema.xml"
        invalid_schema.write_text("<fields></fields>")

        result = runner.invoke(
            app,
            ["csv2parquet", str(tmp_csv_file), "--schema", str(invalid_schema)],
        )
        assert result.exit_code != 0
        # Error message appears in output (stdout/stderr combined in CliRunner)
        assert "Unsupported schema format" in result.output or result.exit_code == 2

    def test_load_schema_missing_fields_key(
        self, tmp_invalid_schema_no_fields: Path, tmp_csv_file: Path
    ) -> None:
        """Test _load_schema with missing 'fields' key."""
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(tmp_csv_file),
                "--schema",
                str(tmp_invalid_schema_no_fields),
            ],
        )
        assert result.exit_code != 0
        assert "fields" in result.output.lower() or result.exit_code == 2

    def test_load_schema_unknown_type(
        self, tmp_invalid_schema_unknown_type: Path, tmp_csv_file: Path
    ) -> None:
        """Test _load_schema with unknown type."""
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(tmp_csv_file),
                "--schema",
                str(tmp_invalid_schema_unknown_type),
            ],
        )
        assert result.exit_code != 0
        assert "Unknown type" in result.output or result.exit_code == 2

    def test_load_schema_missing_name(
        self, tmp_invalid_schema_no_name: Path, tmp_csv_file: Path
    ) -> None:
        """Test _load_schema with missing 'name' in field."""
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(tmp_csv_file),
                "--schema",
                str(tmp_invalid_schema_no_name),
            ],
        )
        assert result.exit_code != 0
        assert "name" in result.output.lower() or result.exit_code == 2

    def test_cast_table_with_schema_basic(self) -> None:
        """Test _cast_table_with_schema basic functionality."""
        # Create a string table
        table = pa.table(
            {
                "id": ["1", "2", "3"],
                "name": ["Alice", "Bob", "Charlie"],
                "value": ["10.5", "20.3", "30.1"],
            }
        )

        type_mapping = {
            "id": pa.int64(),
            "value": pa.float64(),
        }

        result = _cast_table_with_schema(table, type_mapping)

        assert result.schema.field("id").type == pa.int64()
        assert result.schema.field("name").type == pa.string()  # Not in mapping, stays string
        assert result.schema.field("value").type == pa.float64()

    def test_cast_table_with_schema_missing_column(self) -> None:
        """Test _cast_table_with_schema with schema column not in table."""
        from click.exceptions import BadParameter

        table = pa.table({"id": ["1", "2"]})

        type_mapping = {
            "id": pa.int64(),
            "nonexistent": pa.string(),
        }

        with pytest.raises(BadParameter):
            _cast_table_with_schema(table, type_mapping)


class TestCompressionEnum:
    """Tests for Compression enum."""

    def test_compression_values(self) -> None:
        """Test that all compression values are correct."""
        assert Compression.NONE.value == "none"
        assert Compression.SNAPPY.value == "snappy"
        assert Compression.ZSTD.value == "zstd"
        assert Compression.GZIP.value == "gzip"
        assert Compression.LZ4.value == "lz4"

    def test_compression_is_string_enum(self) -> None:
        """Test that Compression is a string enum."""
        assert isinstance(Compression.SNAPPY, str)
        assert Compression.SNAPPY == "snappy"


class TestSchemaTypes:
    """Tests for all supported schema types."""

    def test_timestamp_type(self, tmp_path: Path) -> None:
        """Test timestamp type conversion."""
        csv_path = tmp_path / "timestamp.csv"
        csv_path.write_text("ts\n2024-01-15 10:30:00\n2024-02-20 14:45:00\n")

        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text("fields:\n  - name: ts\n    type: timestamp\n")

        output_path = tmp_path / "output.parquet"
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(csv_path),
                "-o",
                str(output_path),
                "--schema",
                str(schema_path),
            ],
        )
        assert result.exit_code == 0

        table = pq.read_table(output_path)
        assert table.schema.field("ts").type == pa.timestamp("us")

    def test_date_type(self, tmp_path: Path) -> None:
        """Test date type conversion."""
        csv_path = tmp_path / "date.csv"
        csv_path.write_text("dt\n2024-01-15\n2024-02-20\n")

        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text("fields:\n  - name: dt\n    type: date\n")

        output_path = tmp_path / "output.parquet"
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(csv_path),
                "-o",
                str(output_path),
                "--schema",
                str(schema_path),
            ],
        )
        assert result.exit_code == 0

        table = pq.read_table(output_path)
        assert table.schema.field("dt").type == pa.date32()

    def test_boolean_type(self, tmp_path: Path) -> None:
        """Test boolean type conversion."""
        csv_path = tmp_path / "bool.csv"
        csv_path.write_text("flag\ntrue\nfalse\n")

        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text("fields:\n  - name: flag\n    type: boolean\n")

        output_path = tmp_path / "output.parquet"
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(csv_path),
                "-o",
                str(output_path),
                "--schema",
                str(schema_path),
            ],
        )
        assert result.exit_code == 0

        table = pq.read_table(output_path)
        assert table.schema.field("flag").type == pa.bool_()

    def test_default_type_is_string(self, tmp_path: Path) -> None:
        """Test that fields without type default to string."""
        csv_path = tmp_path / "default.csv"
        csv_path.write_text("col1\nvalue1\nvalue2\n")

        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text("fields:\n  - name: col1\n")  # No type specified

        output_path = tmp_path / "output.parquet"
        result = runner.invoke(
            app,
            [
                "csv2parquet",
                str(csv_path),
                "-o",
                str(output_path),
                "--schema",
                str(schema_path),
            ],
        )
        assert result.exit_code == 0

        table = pq.read_table(output_path)
        assert table.schema.field("col1").type == pa.string()


class TestVersionOption:
    """Tests for --version option."""

    def test_version_short_option(self) -> None:
        """Test -v option shows version."""
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert "parquet-tools" in result.stdout
        assert "Python" in result.stdout

    def test_version_long_option(self) -> None:
        """Test --version option shows version."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "parquet-tools" in result.stdout


class TestLibraryModule:
    """Tests for the library module."""

    def test_get_version_returns_string(self) -> None:
        """Test get_version returns a string."""
        from parquet_tools.library import __version__
        from parquet_tools.library.helper_func import get_version

        assert isinstance(__version__, str)
        assert isinstance(get_version(), str)

    def test_version_format(self) -> None:
        """Test version follows semantic versioning format."""
        from parquet_tools.library import __version__

        # Version should be in format X.Y.Z or X.Y.Z.devN etc.
        parts = __version__.split(".")
        assert len(parts) >= 2  # At least major.minor
