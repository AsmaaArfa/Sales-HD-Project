#!/usr/bin/env python3
"""
validate_schemas.py
-------------------
Validates BigQuery schema JSON files for structural correctness.
Used in CI to catch malformed schemas before deployment.
"""

import json
import sys
from pathlib import Path

VALID_TYPES = {"STRING", "INTEGER", "INT64", "FLOAT", "FLOAT64",
               "BOOLEAN", "BOOL", "DATE", "TIMESTAMP", "TIME",
               "DATETIME", "NUMERIC", "BIGNUMERIC", "BYTES", "RECORD", "STRUCT"}
VALID_MODES = {"REQUIRED", "NULLABLE", "REPEATED"}
REQUIRED_KEYS = {"name", "type"}


def validate_field(field: dict, path: str) -> list[str]:
    errors = []
    for key in REQUIRED_KEYS:
        if key not in field:
            errors.append(f"{path}: missing required key '{key}'")

    if "type" in field and field["type"].upper() not in VALID_TYPES:
        errors.append(f"{path}: unknown type '{field['type']}'")

    if "mode" in field and field["mode"].upper() not in VALID_MODES:
        errors.append(f"{path}: unknown mode '{field['mode']}'")

    if field.get("type", "").upper() in ("RECORD", "STRUCT"):
        if "fields" not in field:
            errors.append(f"{path}: RECORD type requires 'fields'")
        else:
            for i, sub in enumerate(field["fields"]):
                errors.extend(validate_field(sub, f"{path}.fields[{i}]"))

    return errors


def validate_schema_file(filepath: Path) -> list[str]:
    errors = []
    try:
        with open(filepath) as f:
            schema = json.load(f)
    except json.JSONDecodeError as e:
        return [f"{filepath}: invalid JSON — {e}"]

    if not isinstance(schema, list):
        return [f"{filepath}: schema must be a JSON array"]

    for i, field in enumerate(schema):
        errors.extend(validate_field(field, f"{filepath}[{i}]"))

    return errors


def main():
    if len(sys.argv) < 2:
        print("Usage: validate_schemas.py <schema_directory>")
        sys.exit(1)

    schema_dir = Path(sys.argv[1])
    schema_files = list(schema_dir.glob("*.json"))

    if not schema_files:
        print(f"No JSON schema files found in {schema_dir}")
        sys.exit(0)

    all_errors = []
    for fp in sorted(schema_files):
        errors = validate_schema_file(fp)
        if errors:
            all_errors.extend(errors)
        else:
            print(f"  ✅ {fp.name}")

    if all_errors:
        print(f"\n❌ Schema validation failed:")
        for e in all_errors:
            print(f"   {e}")
        sys.exit(1)

    print(f"\n✅ All {len(schema_files)} schema file(s) are valid.")


if __name__ == "__main__":
    main()
