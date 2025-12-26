# Schema Guide

This document describes the schema file format for `parquet-tools csv2parquet`.

## Overview

Schema files define column types for CSV to Parquet conversion.
Without a schema, all columns are written as `string` type.

## Supported Formats

- YAML (`.yaml`, `.yml`)
- JSON (`.json`)

## Basic Structure

### YAML Format

```yaml
fields:
  - name: column_name
    type: data_type
  - name: another_column
    type: data_type
```

### JSON Format

```json
{
  "fields": [
    { "name": "column_name", "type": "data_type" },
    { "name": "another_column", "type": "data_type" }
  ]
}
```

## Required Fields

| Field | Required | Description |
| ------- | ---------- | ------------- |
| `name` | Yes | Column name (must match CSV header exactly) |
| `type` | No | Data type (defaults to `string` if omitted) |

## Supported Data Types

| Type | Description | Example Values |
| ------ | ------------- | ---------------- |
| `string` | Text data | `"hello"`, `"2024-01-01"` |
| `int64` | 64-bit integer | `42`, `-100`, `0` |
| `float64` | 64-bit floating point | `3.14`, `-0.5`, `1e10` |
| `boolean` | Boolean value | `true`, `false`, `1`, `0` |
| `timestamp` | Date and time (microsecond) | `2024-01-01 12:00:00` |
| `date` | Date only | `2024-01-01` |

## Rules and Constraints

### 1. Column Name Matching

Column names in the schema **must exactly match** the CSV header names.

```csv
id,user_name,created_at
1,Alice,2024-01-01
```

```yaml
# Correct
fields:
  - name: id
    type: int64
  - name: user_name
    type: string

# Wrong - column name mismatch
fields:
  - name: ID          # CSV has "id", not "ID"
    type: int64
  - name: username    # CSV has "user_name", not "username"
    type: string
```

### 2. Schema Columns Must Exist in CSV

If the schema defines a column that does not exist in the CSV, an error is raised.

```yaml
# Error: "email" column not found in CSV
fields:
  - name: id
    type: int64
  - name: email       # This column doesn't exist in CSV
    type: string
```

### 3. Partial Schema is Allowed

You don't need to define all columns. Undefined columns remain as `string`.

```csv
id,name,age,email
1,Alice,30,alice@example.com
```

```yaml
# Only define types for specific columns
fields:
  - name: id
    type: int64
  - name: age
    type: int64
# "name" and "email" will be string type
```

### 4. Type Conversion Rules

#### Integer (`int64`)

- Valid: `"42"`, `"-100"`, `"0"`
- Invalid: `"3.14"`, `"abc"`, `""`

#### Float (`float64`)

- Valid: `"3.14"`, `"-0.5"`, `"1e10"`, `"42"`
- Invalid: `"abc"`, `""`

#### Boolean (`boolean`)

- Valid: `"true"`, `"false"`, `"1"`, `"0"`, `"True"`, `"False"`
- Invalid: `"yes"`, `"no"`, `"abc"`

#### Timestamp (`timestamp`)

- Valid: `"2024-01-01 12:00:00"`, `"2024-01-01T12:00:00"`
- Precision: microseconds
- Invalid: `"01/01/2024"`, `"abc"`

#### Date (`date`)

- Valid: `"2024-01-01"`, `"2024-12-31"`
- Invalid: `"01/01/2024"`, `"2024-1-1"`, `"abc"`

### 5. Null Handling

Empty strings in CSV are treated as `null` values in Parquet.

```csv
id,name
1,Alice
2,
3,Bob
```

Row 2's `name` column will be `null` in the output Parquet file.

## Examples

### Example 1: User Data

**CSV (`users.csv`):**

```csv
id,username,email,age,is_active,created_at
1,alice,alice@example.com,30,true,2024-01-15 10:30:00
2,bob,bob@example.com,25,false,2024-02-20 14:45:00
```

**Schema (`users_schema.yaml`):**

```yaml
fields:
  - name: id
    type: int64
  - name: username
    type: string
  - name: email
    type: string
  - name: age
    type: int64
  - name: is_active
    type: boolean
  - name: created_at
    type: timestamp
```

**Command:**

```bash
parquet-tools csv2parquet users.csv --schema users_schema.yaml
```

### Example 2: Sales Data (JSON Schema)

**CSV (`sales.csv`):**

```csv
order_id,product,quantity,price,order_date
1001,Widget,5,19.99,2024-03-01
1002,Gadget,2,49.99,2024-03-02
```

**Schema (`sales_schema.json`):**

```json
{
  "fields": [
    { "name": "order_id", "type": "int64" },
    { "name": "product", "type": "string" },
    { "name": "quantity", "type": "int64" },
    { "name": "price", "type": "float64" },
    { "name": "order_date", "type": "date" }
  ]
}
```

**Command:**

```bash
parquet-tools csv2parquet sales.csv --schema sales_schema.json -c zstd
```

### Example 3: Minimal Schema (Partial Typing)

**CSV (`data.csv`):**

```csv
id,name,value,description
1,item1,100,Some description
2,item2,200,Another description
```

**Schema (`minimal_schema.yaml`):**

```yaml
# Only type the columns that need specific types
# "name" and "description" will remain as string
fields:
  - name: id
    type: int64
  - name: value
    type: float64
```

## Troubleshooting

### Error: "Schema defines columns not found in CSV"

**Cause:** A column in your schema doesn't exist in the CSV file.

**Solution:** Check column names match exactly (case-sensitive).

### Error: "Unknown type 'xxx'"

**Cause:** Using an unsupported type name.

**Solution:** Use one of: `string`, `int64`, `float64`, `boolean`,
`timestamp`, `date`

### Error: Cast failed for column

**Cause:** Data in CSV cannot be converted to the specified type.

**Solution:** Check your data for invalid values (e.g., text in numeric columns).

## Best Practices

1. **Start with string types** - Convert to specific types only when needed
2. **Validate data first** - Ensure CSV data matches expected types
3. **Use YAML for readability** - Easier to maintain than JSON
4. **Document your schema** - Add comments in YAML files
5. **Version control schemas** - Keep schemas alongside your data pipelines
