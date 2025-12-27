# Testing Guide

This document provides detailed information about the test suite for parquet-tools.

## Overview

The test suite uses [pytest](https://docs.pytest.org/) as the testing framework
and covers all CLI commands, utility functions, and library modules.

## Setup

### Install Development Dependencies

```bash
uv sync --group dev
```

This installs:

- `pytest>=8.0.0` - Testing framework
- `pytest-cov>=4.0.0` - Coverage reporting

## Running Tests

### Basic Commands

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run a specific test file
uv run pytest tests/test_cli.py

# Run a specific test class
uv run pytest tests/test_cli.py::TestHeadCommand

# Run a specific test method
uv run pytest tests/test_cli.py::TestHeadCommand::test_head_default_rows
```

### Coverage Reports

```bash
# Terminal coverage report
uv run pytest --cov=parquet_tools --cov-report=term-missing

# HTML coverage report
uv run pytest --cov=parquet_tools --cov-report=html
# Open htmlcov/index.html in browser

# XML coverage report (for CI)
uv run pytest --cov=parquet_tools --cov-report=xml
```

### Other Useful Options

```bash
# Stop on first failure
uv run pytest -x

# Run last failed tests only
uv run pytest --lf

# Show local variables in tracebacks
uv run pytest -l

# Run tests matching a keyword
uv run pytest -k "head"
```

## Test Structure

```text
tests/
├── __init__.py          # Package marker
├── conftest.py          # Shared fixtures
└── test_cli.py          # CLI command tests
```

### conftest.py - Fixtures

The `conftest.py` file contains pytest fixtures that provide reusable test data:

| Fixture | Description |
| ------- | ----------- |
| `tmp_parquet_file` | Simple parquet file with 5 rows (snappy compression) |
| `tmp_parquet_file_gzip` | Parquet file with gzip compression |
| `tmp_parquet_file_no_compression` | Parquet file without compression |
| `tmp_parquet_dir` | Directory with 3 parquet files for merge testing |
| `tmp_csv_file` | Simple CSV file with 3 rows |
| `tmp_csv_file_with_types` | CSV file with various data types |
| `tmp_yaml_schema` | YAML schema file for type mapping |
| `tmp_json_schema` | JSON schema file for type mapping |
| `tmp_invalid_schema_no_fields` | Invalid schema (missing 'fields' key) |
| `tmp_invalid_schema_unknown_type` | Invalid schema (unknown type) |
| `tmp_invalid_schema_no_name` | Invalid schema (missing field name) |
| `tmp_empty_parquet_dir` | Empty directory for error testing |
| `large_parquet_file` | Parquet file with 100 rows |
| `tmp_csv_with_null_values` | CSV file with various null representations |

### test_cli.py - Test Classes

#### TestHeadCommand

Tests for the `head` command functionality.

| Test Method | Description |
| ----------- | ----------- |
| `test_head_default_rows` | Verify default 10 rows display |
| `test_head_limited_rows` | Verify `-n` option limits output |
| `test_head_output_to_csv` | Verify `-o` option exports to CSV |
| `test_head_file_not_found` | Verify error handling for missing files |
| `test_head_with_rows_option_long_form` | Verify `--rows` option works |

#### TestInfoCommand

Tests for the `info` command functionality.

| Test Method | Description |
| ----------- | ----------- |
| `test_info_basic` | Verify basic metadata display |
| `test_info_yaml_output` | Verify `--yaml` output format |
| `test_info_json_output` | Verify `--json` output format |
| `test_info_yaml_json_mutual_exclusion` | Verify error when both --yaml and --json provided |
| `test_info_shows_compression` | Verify compression codec display |
| `test_info_file_not_found` | Verify error handling for missing files |

#### TestMergeCommand

Tests for the `merge` command functionality.

| Test Method | Description |
| ----------- | ----------- |
| `test_merge_basic` | Verify basic merge operation |
| `test_merge_default_output_path` | Verify default output path naming |
| `test_merge_with_compression` | Verify all compression codecs work |
| `test_merge_empty_directory` | Verify error handling for empty directories |

#### TestCsv2ParquetCommand

Tests for the `csv2parquet` command functionality.

| Test Method | Description |
| ----------- | ----------- |
| `test_csv2parquet_basic` | Verify basic CSV to Parquet conversion |
| `test_csv2parquet_default_output` | Verify default output path naming |
| `test_csv2parquet_with_yaml_schema` | Verify YAML schema type casting |
| `test_csv2parquet_with_json_schema` | Verify JSON schema type casting |
| `test_csv2parquet_all_string_without_schema` | Verify default string type behavior |
| `test_csv2parquet_with_compression` | Verify all compression codecs work |
| `test_csv2parquet_file_not_found` | Verify error handling for missing files |
| `test_csv2parquet_schema_not_found` | Verify error handling for missing schema |
| `test_csv2parquet_warns_non_csv_extension` | Verify warning for non-.csv files |

#### TestUtilityFunctions

Tests for internal utility functions.

| Test Method | Description |
| ----------- | ----------- |
| `test_get_compression_info_snappy` | Verify snappy compression detection |
| `test_get_compression_info_gzip` | Verify gzip compression detection |
| `test_get_compression_info_none` | Verify uncompressed detection |
| `test_load_schema_yaml` | Verify YAML schema loading |
| `test_load_schema_json` | Verify JSON schema loading |
| `test_load_schema_invalid_format` | Verify error for unsupported formats |
| `test_load_schema_missing_fields_key` | Verify error for missing 'fields' key |
| `test_load_schema_unknown_type` | Verify error for unknown types |
| `test_load_schema_missing_name` | Verify error for missing field names |
| `test_cast_table_with_schema_basic` | Verify table type casting |
| `test_cast_table_with_schema_missing_column` | Verify error for missing columns |

#### TestCompressionEnum

Tests for the `Compression` enum.

| Test Method | Description |
| ----------- | ----------- |
| `test_compression_values` | Verify enum values are correct |
| `test_compression_is_string_enum` | Verify enum is string-based |

#### TestSchemaTypes

Tests for schema type conversions.

| Test Method | Description |
| ----------- | ----------- |
| `test_timestamp_type` | Verify timestamp conversion |
| `test_date_type` | Verify date conversion |
| `test_boolean_type` | Verify boolean conversion |
| `test_default_type_is_string` | Verify default string type |

#### TestQueryCommand

Tests for the `query` command functionality (SQL queries via DuckDB).

| Test Method | Description |
| ----------- | ----------- |
| `test_query_basic` | Verify basic SQL query execution |
| `test_query_with_filter` | Verify SQL query with WHERE clause |
| `test_query_with_aggregation` | Verify SQL query with aggregation |
| `test_query_output_to_csv` | Verify `-o` option exports to CSV |
| `test_query_from_sql_file` | Verify `--sql-file` option works |
| `test_query_sql_file_with_output` | Verify SQL file with CSV output |
| `test_query_file_not_found` | Verify error for missing parquet file |
| `test_query_sql_file_not_found` | Verify error for missing SQL file |
| `test_query_both_sql_and_file_error` | Verify error when both SQL string and file provided |
| `test_query_no_sql_error` | Verify error when no SQL input provided |
| `test_query_invalid_sql` | Verify error handling for invalid SQL |
| `test_query_empty_sql_file` | Verify error for empty SQL file |

#### TestCsv2ParquetNullHandling

Tests for NULL/NA handling in csv2parquet command.

| Test Method | Description |
| ----------- | ----------- |
| `test_null_values_in_string_column` | Verify various null representations become null |
| `test_null_values_preserved_after_schema_cast` | Verify nulls preserved after type casting |
| `test_empty_string_becomes_null` | Verify empty string becomes null |
| `test_na_variants_become_null` | Verify NA, N/A, n/a, #N/A become null |
| `test_null_variants_become_null` | Verify NULL, null become null |
| `test_nan_variants_become_null` | Verify NaN, nan, -NaN, -nan become null |
| `test_null_count_in_parquet` | Verify null count is correct in output |

#### TestVersionOption

Tests for the `--version` option.

| Test Method | Description |
| ----------- | ----------- |
| `test_version_short_option` | Verify `-v` shows version |
| `test_version_long_option` | Verify `--version` shows version |

#### TestLibraryModule

Tests for the library module.

| Test Method | Description |
| ----------- | ----------- |
| `test_get_version_returns_string` | Verify version is a string |
| `test_version_format` | Verify semantic versioning format |

## Writing New Tests

### Adding a New Test

1. Add test method to appropriate test class in `test_cli.py`
2. Use existing fixtures from `conftest.py` or create new ones
3. Follow naming convention: `test_<feature>_<scenario>`

### Example Test

```python
def test_head_with_custom_rows(self, tmp_parquet_file: Path) -> None:
    """Test head command with custom row count."""
    result = runner.invoke(app, ["head", str(tmp_parquet_file), "-n", "3"])
    assert result.exit_code == 0
    # Add specific assertions
```

### Adding a New Fixture

Add to `conftest.py`:

```python
@pytest.fixture
def my_fixture(tmp_path: Path) -> Path:
    """Description of the fixture."""
    # Setup code
    file_path = tmp_path / "test_file.parquet"
    # Create test data
    return file_path
```

## Testing CLI Commands

The test suite uses `typer.testing.CliRunner` to invoke CLI commands:

```python
from typer.testing import CliRunner
from parquet_tools.cli import app

runner = CliRunner()

def test_example():
    result = runner.invoke(app, ["command", "arg1", "--option", "value"])
    assert result.exit_code == 0
    assert "expected output" in result.stdout
```

### Key Properties

- `result.exit_code` - Command exit code (0 = success)
- `result.stdout` - Standard output
- `result.output` - Combined stdout and stderr
- `result.exception` - Exception if command raised one

## Continuous Integration

For CI environments, use:

```bash
# Run tests with JUnit XML output
uv run pytest --junitxml=test-results.xml

# Run with coverage XML for code coverage tools
uv run pytest --cov=parquet_tools --cov-report=xml
```
