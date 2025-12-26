"""Pytest fixtures for parquet-tools tests."""

import json
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def tmp_parquet_file(tmp_path: Path) -> Path:
    """Create a simple parquet file for testing."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [10.5, 20.3, 30.1, 40.7, 50.9],
        }
    )
    file_path = tmp_path / "test.parquet"
    df.to_parquet(file_path, compression="snappy")
    return file_path


@pytest.fixture
def tmp_parquet_file_gzip(tmp_path: Path) -> Path:
    """Create a parquet file with gzip compression."""
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    file_path = tmp_path / "test_gzip.parquet"
    df.to_parquet(file_path, compression="gzip")
    return file_path


@pytest.fixture
def tmp_parquet_file_no_compression(tmp_path: Path) -> Path:
    """Create a parquet file without compression."""
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    file_path = tmp_path / "test_none.parquet"
    df.to_parquet(file_path, compression=None)
    return file_path


@pytest.fixture
def tmp_parquet_dir(tmp_path: Path) -> Path:
    """Create a directory with multiple parquet files for merge testing."""
    parquet_dir = tmp_path / "parquet_files"
    parquet_dir.mkdir()

    # Create multiple parquet files with same schema
    for i in range(3):
        df = pd.DataFrame(
            {
                "id": [i * 10 + 1, i * 10 + 2, i * 10 + 3],
                "name": [f"name_{i}_1", f"name_{i}_2", f"name_{i}_3"],
            }
        )
        df.to_parquet(parquet_dir / f"file_{i}.parquet")

    return parquet_dir


@pytest.fixture
def tmp_csv_file(tmp_path: Path) -> Path:
    """Create a simple CSV file for testing."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("id,name,value\n1,Alice,10.5\n2,Bob,20.3\n3,Charlie,30.1\n")
    return csv_path


@pytest.fixture
def tmp_csv_file_with_types(tmp_path: Path) -> Path:
    """Create a CSV file with various data types for schema testing."""
    csv_path = tmp_path / "typed.csv"
    csv_path.write_text(
        "id,name,amount,active,created_at,birth_date\n"
        "1,Alice,100.50,true,2024-01-15 10:30:00,2000-01-15\n"
        "2,Bob,200.75,false,2024-02-20 14:45:00,1995-06-20\n"
        "3,Charlie,300.25,true,2024-03-25 09:15:00,1990-12-01\n"
    )
    return csv_path


@pytest.fixture
def tmp_yaml_schema(tmp_path: Path) -> Path:
    """Create a YAML schema file for testing."""
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(
        """fields:
  - name: id
    type: int64
  - name: name
    type: string
  - name: amount
    type: float64
  - name: active
    type: boolean
"""
    )
    return schema_path


@pytest.fixture
def tmp_json_schema(tmp_path: Path) -> Path:
    """Create a JSON schema file for testing."""
    schema_path = tmp_path / "schema.json"
    schema_data = {
        "fields": [
            {"name": "id", "type": "int64"},
            {"name": "name", "type": "string"},
            {"name": "value", "type": "float64"},
        ]
    }
    schema_path.write_text(json.dumps(schema_data))
    return schema_path


@pytest.fixture
def tmp_invalid_schema_no_fields(tmp_path: Path) -> Path:
    """Create a schema file without 'fields' key."""
    schema_path = tmp_path / "invalid_schema.yaml"
    schema_path.write_text("columns:\n  - name: id\n")
    return schema_path


@pytest.fixture
def tmp_invalid_schema_unknown_type(tmp_path: Path) -> Path:
    """Create a schema file with unknown type."""
    schema_path = tmp_path / "invalid_type.yaml"
    schema_path.write_text("fields:\n  - name: id\n    type: unknown_type\n")
    return schema_path


@pytest.fixture
def tmp_invalid_schema_no_name(tmp_path: Path) -> Path:
    """Create a schema file with a field missing 'name'."""
    schema_path = tmp_path / "no_name.yaml"
    schema_path.write_text("fields:\n  - type: int64\n")
    return schema_path


@pytest.fixture
def tmp_empty_parquet_dir(tmp_path: Path) -> Path:
    """Create an empty directory for testing merge with no files."""
    empty_dir = tmp_path / "empty_dir"
    empty_dir.mkdir()
    return empty_dir


@pytest.fixture
def large_parquet_file(tmp_path: Path) -> Path:
    """Create a larger parquet file for head command testing."""
    df = pd.DataFrame(
        {
            "id": range(100),
            "value": [f"value_{i}" for i in range(100)],
        }
    )
    file_path = tmp_path / "large.parquet"
    df.to_parquet(file_path)
    return file_path
