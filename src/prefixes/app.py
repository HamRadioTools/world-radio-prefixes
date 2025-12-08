#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=W0102,E0712,C0103,R0903

"""World Radio Prefixes - RCLDX companion software"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import csv
import json

from config import get_config  # noqa: F401


__updated__ = "2025-12-08 14:43:46"


###############################################################################
#
# DATA STRUCTURES AND VARIABLES
#
###############################################################################


@dataclass(frozen=True)
class PrefixInfo:
    """Information associated with a radio prefix."""

    prefix: str
    name: str
    dxcc: Optional[int]
    country_code: str
    continent: str
    cq_zones: Optional[str]


HERE = Path(__file__).resolve().parent
RESOURCES_DIR = HERE / "resources"
CSV_PATH = RESOURCES_DIR / "prefixes.csv"

# Output directory inside the package
OUTPUT_DIR = HERE / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# JSON will now be written here: src/prefixes/output/prefixes.json
DEFAULT_JSON_PATH = OUTPUT_DIR / "prefixes.json"

# Lazy globals so this module can be imported cheaply
_PREFIX_MAP: Optional[Dict[str, PrefixInfo]] = None
_MAX_PREFIX_LEN: int = 0


###############################################################################
#
# SUPPORT FUNCTIONS
#
###############################################################################


def _parse_int(value: str) -> Optional[int]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _resolve_columns(fieldnames: list[str]) -> dict[str, Optional[str]]:
    """
    Map logical column names to real CSV headers, handling BOM and case/spacing.

    Returns a dict with keys:
        prefix, name, dxcc, country_code, continent, cq_zones, likely_prefixes
    and values = header name in the CSV (or None if not found).
    """

    # Normalizamos los nombres: quitamos BOM, espacios, bajamos a lower…
    norm = {(name or "").lstrip("\ufeff").strip().lower().replace(" ", "_"): name for name in fieldnames}

    def col(key: str, *alts: str) -> Optional[str]:
        for k in (key, *alts):
            n = k.lower().replace(" ", "_")
            if n in norm:
                return norm[n]
        return None

    return {
        "prefix": col("prefix"),
        "name": col("short_name", "short name"),
        "dxcc": col("adif_dxcc_code", "adif_dxcc code", "adif"),
        "country_code": col("country_code", "country code"),
        "continent": col("continent"),
        "cq_zones": col("cq_zones", "cq zones"),
        "likely_prefixes": col("likely_prefixes", "likely prefixes", "prefixes"),
    }


def load_prefix_table(
    csv_path: Path = CSV_PATH,
) -> Dict[str, PrefixInfo]:
    """
    Load prefixes from the CSV file and return a mapping:
    prefix (e.g. "UA7") -> PrefixInfo.

    This:
    - Reads the main 'Prefix' column.
    - Expands all comma-separated entries in 'Likely Prefixes'.
    - Skips empty and '???' entries.
    - Ensures each prefix maps to a PrefixInfo with DXCC, country, continent, CQ zone.
    """
    prefix_map: Dict[str, PrefixInfo] = {}

    with csv_path.open(newline="", encoding="utf-8") as f:
        # Sniffer por si cambias el delimitador en el futuro
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.get_dialect("excel")

        reader = csv.DictReader(f, dialect=dialect)

        if not reader.fieldnames:
            # CSV vacío o mal formado
            return {}

        cols = _resolve_columns(reader.fieldnames)

        prefix_col = cols["prefix"]
        name_col = cols["name"]
        dxcc_col = cols["dxcc"]
        country_col = cols["country_code"]
        continent_col = cols["continent"]
        cq_col = cols["cq_zones"]
        likely_col = cols["likely_prefixes"]

        if prefix_col is None:
            # Sin columna Prefix no podemos hacer nada útil
            return {}

        for row in reader:
            base_prefix = (row.get(prefix_col) or "").strip().upper()
            if not base_prefix:
                continue

            name = (row.get(name_col) or "").strip() if name_col else ""
            dxcc = _parse_int(row.get(dxcc_col) or "") if dxcc_col else None
            country_code = (row.get(country_col) or "").strip().lower() if country_col else ""
            continent = (row.get(continent_col) or "").strip().upper() if continent_col else ""
            cq_zones_raw = row.get(cq_col) if cq_col else None
            cq_zones = str(cq_zones_raw).strip() if cq_zones_raw not in (None, "") else None

            base_info = PrefixInfo(
                prefix=base_prefix,
                name=name,
                dxcc=dxcc,
                country_code=country_code,
                continent=continent,
                cq_zones=cq_zones,
            )

            # 1) Main prefix
            prefix_map[base_prefix] = base_info

            # 2) Expand Likely Prefixes
            likely = row.get(likely_col) if likely_col else None
            if isinstance(likely, str) and likely.strip():
                for raw in likely.split(","):
                    pref = raw.strip().upper()
                    if not pref:
                        continue
                    if pref == "???":
                        continue
                    prefix_map[pref] = PrefixInfo(
                        prefix=pref,
                        name=name,
                        dxcc=dxcc,
                        country_code=country_code,
                        continent=continent,
                        cq_zones=cq_zones,
                    )

    return prefix_map


def get_prefix_map() -> Dict[str, PrefixInfo]:
    """Return the global prefix map, loading it from CSV on first use."""
    global _PREFIX_MAP, _MAX_PREFIX_LEN
    if _PREFIX_MAP is None:
        prefix_map = load_prefix_table()
        _PREFIX_MAP = prefix_map
        _MAX_PREFIX_LEN = max((len(p) for p in prefix_map.keys()), default=0)
    return _PREFIX_MAP


def get_max_prefix_length() -> int:
    """Return the maximum prefix length seen in the table."""
    # Ensure the map is loaded
    get_prefix_map()
    return _MAX_PREFIX_LEN


def export_prefixes_json(
    json_path: Path = DEFAULT_JSON_PATH,
    prefix_map: Optional[Dict[str, PrefixInfo]] = None,
) -> None:
    """
    Export the prefix table to a JSON file suitable for
    in-memory loading or Redis insertion.

    JSON structure:
        {
          "UA7": {
            "name": "...",
            "dxcc": 54,
            "country_code": "ru",
            "continent": "EU",
            "cq_zones": "16"
          },
          ...
        }
    """
    if prefix_map is None:
        prefix_map = get_prefix_map()

    payload = {
        prefix: {
            "name": info.name,
            "dxcc": info.dxcc,
            "country_code": info.country_code,
            "continent": info.continent,
            "cq_zones": info.cq_zones,
        }
        for prefix, info in prefix_map.items()
    }

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


###############################################################################
#
# APPLICATION MAIN
#
###############################################################################


def main() -> None:
    """
    Main application code
    """

    # Simple behaviour when running: regenerate prefixes.json into ./output
    export_prefixes_json()
    print(f"Exported prefix table to {DEFAULT_JSON_PATH}")


###############################################################################
#
# APPLICATION ENTRY POINT
#
###############################################################################

if __name__ == "__main__":
    main()
