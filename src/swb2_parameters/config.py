"""TOML configuration loader for swb2_parameters."""
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TableSpec:
    """Definition of one output table."""
    families: list[str] = field(default_factory=list)
    singletons: list[str] = field(default_factory=list)


@dataclass
class Config:
    """Parsed project configuration."""
    hsg_count: int
    groups: dict[str, list[int]]  # group_name -> code list
    tables: dict[str, TableSpec]  # table_name -> spec


def load_config(path: str | Path) -> Config:
    """Load and validate a project TOML file.

    Args:
        path: Path to the TOML configuration file.

    Returns:
        Parsed Config object.

    Raises:
        FileNotFoundError: If the TOML file doesn't exist.
        ValueError: If required keys are missing or invalid.
    """
    path = Path(path)
    with path.open("rb") as f:
        raw = tomllib.load(f)

    # [long_schema]
    schema = raw.get("long_schema", {})
    hsg_count = int(schema.get("hsg_count", 7))

    # [groups.*]
    groups_raw = raw.get("groups", {})
    groups = {name: spec["codes"] for name, spec in groups_raw.items() if "codes" in spec}

    # [tables.*]
    tables_raw = raw.get("tables", {})
    if not tables_raw:
        raise ValueError("Config must define at least one [tables.<name>] section.")
    tables = {}
    for name, spec in tables_raw.items():
        tables[name] = TableSpec(
            families=spec.get("families", []),
            singletons=spec.get("singletons", []),
        )

    return Config(
        hsg_count=hsg_count,
        groups=groups,
        tables=tables,
    )
