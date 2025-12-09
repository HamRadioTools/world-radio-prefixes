#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=W0102,E0712,C0103,R0903

"""World Radio Prefixes - RCLDX companion software"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import argparse
import csv
import json
import redis
from config import get_config

__updated__ = "2025-12-09 02:10:23"

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

# Load config ONCE – it already pulls from env + dotenv inside config.py
CONFIG: dict = get_config() or {}


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
    """
    prefix_map: Dict[str, PrefixInfo] = {}

    if not csv_path.exists():
        print(f"[ERROR] CSV file not found at {csv_path}")
        return prefix_map

    with csv_path.open(newline="", encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.get_dialect("excel")

        reader = csv.DictReader(f, dialect=dialect)

        if not reader.fieldnames:
            print("[ERROR] prefixes.csv has no header row")
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
            print("[ERROR] Could not find 'Prefix' column in CSV")
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
    get_prefix_map()
    return _MAX_PREFIX_LEN


def export_prefixes_json(
    json_path: Path = DEFAULT_JSON_PATH,
    prefix_map: Optional[Dict[str, PrefixInfo]] = None,
) -> None:
    """
    Export the prefix table to a JSON file suitable for
    in-memory loading or Redis insertion.
    """
    if prefix_map is None:
        prefix_map = get_prefix_map()

    if not prefix_map:
        print("[WARN] No prefixes loaded, JSON will be empty")

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
    print(f"[INFO] Exported {len(payload)} prefixes to {json_path}")


def load_prefixes_json(json_path: Path = DEFAULT_JSON_PATH) -> Dict[str, dict]:
    """Load the generated prefixes.json into a plain dict."""
    if not json_path.exists():
        print(f"[ERROR] JSON file not found at {json_path}")
        return {}
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            print(f"[ERROR] JSON at {json_path} is not an object")
            return {}
        return data
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Failed to parse JSON at {json_path}: {exc}")
        return {}


###############################################################################
# REDIS HELPERS (NO DOUBLE CONFIG / ENV LOADING)
###############################################################################


def _get_redis_client():
    """
    Build and return a Redis client using CONFIG.

    CONFIG comes from get_config() once, which already:
      - loads .env (via config.py)
      - reads environment variables
    """
    if redis is None:
        print("[ERROR] redis library is not installed. Run: pip install redis")
        return None

    host = CONFIG.get("REDIS_HOST")
    port = CONFIG.get("REDIS_PORT")
    db = CONFIG.get("REDIS_DB")
    password = CONFIG.get("REDIS_PASSWORD")

    # Normalize port/db
    try:
        if isinstance(port, str):
            port = int(port)
    except ValueError:
        port = None

    try:
        if isinstance(db, str):
            db = int(db)
    except ValueError:
        db = None

    port = port or 6379
    db = db or 0

    # Treat "none" / "" as no password
    if isinstance(password, str) and password.lower() in ("", "none", "null"):
        password = None

    if not host:
        print("[ERROR] Redis host not configured (REDIS_HOST in config/env)")
        return None

    try:
        client = redis.Redis(host=host, port=port, db=db, password=password)
        client.ping()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Could not connect to Redis at {host}:{port} db={db}: {exc}")
        return None

    print(f"[INFO] Connected to Redis at {host}:{port}/{db}")
    return client


def inject_prefixes_into_redis(prefixes: Dict[str, dict]) -> None:
    """Insert all prefixes into Redis as hashes."""
    if not prefixes:
        print("[ERROR] No prefixes to inject into Redis")
        return

    client = _get_redis_client()
    if client is None:
        return

    print(f"[INFO] Injecting {len(prefixes)} prefixes into Redis")

    pipe = client.pipeline(transaction=False)
    count = 0

    for prefix, info in prefixes.items():
        key = f"rcldx:prefix:{prefix}"
        mapping = {k: ("" if v is None else str(v)) for k, v in info.items()}
        pipe.hset(key, mapping=mapping)
        count += 1
        if count % 1000 == 0:
            pipe.execute()

    pipe.execute()
    print(f"[INFO] Injected {count} prefix entries into Redis")


def clean_local_files() -> None:
    """Remove local generated files (currently prefixes.json)."""
    if DEFAULT_JSON_PATH.exists():
        DEFAULT_JSON_PATH.unlink()
        print(f"[INFO] Removed local file {DEFAULT_JSON_PATH}")
    else:
        print(f"[INFO] No local prefixes.json found at {DEFAULT_JSON_PATH}; nothing to clean")


def clean_redis_prefixes() -> None:
    """Remove Redis keys for RCLDX prefixes (rcldx:prefix:*)."""
    client = _get_redis_client()
    if client is None:
        # _get_redis_client already printed an error
        return

    pattern = "rcldx:prefix:*"
    print(f"[INFO] Searching for keys matching '{pattern}' in Redis")

    # Use SCAN to iterate keys
    keys = list(client.scan_iter(match=pattern))
    if not keys:
        print(f"[INFO] No keys matching '{pattern}' found in Redis; nothing to clean")
        return

    deleted = client.delete(*keys)
    print(f"[INFO] Deleted {deleted} Redis keys matching '{pattern}'")


###############################################################################
#
# APPLICATION MAIN
#
###############################################################################


def main(argv: Optional[list[str]] = None) -> None:
    """
    CLI usage:
      - No arguments: print help
      - --parsecsv           : parse CSV and generate prefixes.json
      - --injectredis        : load prefixes.json and inject into Redis
      - --clean {local,redis,all} : remove local JSON, Redis keys, or both

      All flags can be combined; operations run in this order:
        1) clean
        2) parsecsv
        3) injectredis
    """
    parser = argparse.ArgumentParser(description="World Radio Prefixes - CSV to JSON and Redis injector")
    parser.add_argument(
        "--parsecsv",
        action="store_true",
        help="Parse prefixes.csv from resources and generate prefixes.json in output/",
    )
    parser.add_argument(
        "--injectredis",
        action="store_true",
        help="Load prefixes.json from output/ and inject into Redis",
    )
    parser.add_argument(
        "--clean",
        choices=["local", "redis", "all"],
        help="Clean generated prefixes.json, Redis prefixes, or both",
    )

    args = parser.parse_args(argv)

    if not args.parsecsv and not args.injectredis and not args.clean:
        parser.print_help()
        return

    # 1) Cleaning step
    if args.clean:
        if args.clean in ("local", "all"):
            clean_local_files()
        if args.clean in ("redis", "all"):
            clean_redis_prefixes()

    # 2) Parse CSV → JSON
    if args.parsecsv:
        print(f"[INFO] Parsing CSV from {CSV_PATH}")
        prefix_map = load_prefix_table()
        export_prefixes_json(DEFAULT_JSON_PATH, prefix_map)

    # 3) JSON → Redis
    if args.injectredis:
        print(f"[INFO] Loading prefixes from JSON at {DEFAULT_JSON_PATH}")
        prefixes = load_prefixes_json(DEFAULT_JSON_PATH)
        inject_prefixes_into_redis(prefixes)


###############################################################################
#
# APPLICATION ENTRY POINT
#
###############################################################################

if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
