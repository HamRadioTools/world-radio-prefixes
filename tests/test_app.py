#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""World Radio Prefixes - RCLDX companion software"""

__updated__ = "2025-12-09 02:24:33"

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from prefixes import app


# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


class FakePipeline:
    def __init__(self):
        self.hset_calls: List[Dict[str, Any]] = []
        self.executed = 0

    def hset(self, key: str, mapping: Dict[str, str]):
        self.hset_calls.append({"key": key, "mapping": mapping})

    def execute(self):
        self.executed += 1


class FakeRedisClient:
    def __init__(self, keys=None):
        # keys: list of keys present in the fake DB
        self._keys = set(keys or [])
        self.pipeline_obj = FakePipeline()
        self.deleted: List[str] = []
        self.ping_called = False

    # Used by _get_redis_client() when not monkeypatched
    def ping(self):
        self.ping_called = True

    # Used by inject_prefixes_into_redis()
    def pipeline(self, transaction=False):
        return self.pipeline_obj

    # Used by clean_redis_prefixes()
    def scan_iter(self, match: str):
        # Very simple "match rcldx:prefix:*"
        prefix = match.replace("*", "")
        for k in list(self._keys):
            if k.startswith(prefix):
                yield k

    def delete(self, *keys):
        for k in keys:
            if k in self._keys:
                self._keys.remove(k)
                self.deleted.append(k)
        return len(keys)


# ---------------------------------------------------------------------------
# CSV → PrefixInfo map
# ---------------------------------------------------------------------------


def test_load_prefix_table_parses_prefix_and_likely_prefixes(tmp_path: Path):
    """
    Ensure load_prefix_table:
      - Handles BOM in 'Prefix' column
      - Expands Likely Prefixes
    """
    csv_content = (
        "\ufeffPrefix,Short Name,ADIF DXCC Code,Country Code,Continent,CQ Zones,Likely Prefixes\n"
        'EA,Spain,281,es,EU,14,"EA1, EA2"\n'
    )
    csv_path = tmp_path / "prefixes.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    prefix_map = app.load_prefix_table(csv_path=csv_path)

    assert "EA" in prefix_map
    assert "EA1" in prefix_map
    assert "EA2" in prefix_map

    ea = prefix_map["EA"]
    assert ea.name == "Spain"
    assert ea.dxcc == 281
    assert ea.country_code == "es"
    assert ea.continent == "EU"
    assert ea.cq_zones == "14"

    ea1 = prefix_map["EA1"]
    assert ea1.name == "Spain"
    assert ea1.dxcc == 281


# ---------------------------------------------------------------------------
# JSON export / import
# ---------------------------------------------------------------------------


def test_export_and_load_prefixes_json_roundtrip(tmp_path: Path):
    """export_prefixes_json + load_prefixes_json should round-trip correctly."""
    json_path = tmp_path / "prefixes.json"

    prefix_map = {
        "EA": app.PrefixInfo(
            prefix="EA",
            name="Spain",
            dxcc=281,
            country_code="es",
            continent="EU",
            cq_zones="14",
        )
    }

    app.export_prefixes_json(json_path=json_path, prefix_map=prefix_map)

    assert json_path.exists()

    data = app.load_prefixes_json(json_path=json_path)
    assert "EA" in data
    assert data["EA"]["name"] == "Spain"
    assert data["EA"]["dxcc"] == 281
    assert data["EA"]["continent"] == "EU"


def test_load_prefixes_json_missing_file(tmp_path: Path):
    """Missing JSON should fail gracefully and return {}."""
    json_path = tmp_path / "does_not_exist.json"
    data = app.load_prefixes_json(json_path=json_path)
    assert data == {}


# ---------------------------------------------------------------------------
# clean_local_files
# ---------------------------------------------------------------------------


def test_clean_local_files_existing_and_missing(tmp_path: Path, monkeypatch):
    """clean_local_files should remove file if exists, and be graceful if not."""

    # Point DEFAULT_JSON_PATH to our temp file
    fake_json = tmp_path / "prefixes.json"
    fake_json.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(app, "DEFAULT_JSON_PATH", fake_json)

    # First call: file exists and should be deleted
    app.clean_local_files()
    assert not fake_json.exists()

    # Second call: no file, should not raise
    app.clean_local_files()
    assert not fake_json.exists()


# ---------------------------------------------------------------------------
# inject_prefixes_into_redis
# ---------------------------------------------------------------------------


def test_inject_prefixes_into_redis_no_prefixes(capsys):
    """inject_prefixes_into_redis with {} should not crash."""
    app.inject_prefixes_into_redis({})
    captured = capsys.readouterr()
    assert "No prefixes to inject" in captured.out


def test_inject_prefixes_into_redis_uses_client(monkeypatch):
    """
    inject_prefixes_into_redis should:
      - Use a Redis client via _get_redis_client
      - Store prefixes as hashes with stringified values
    """

    fake_client = FakeRedisClient()

    def fake_get_client():
        return fake_client

    monkeypatch.setattr(app, "_get_redis_client", fake_get_client)

    prefixes = {
        "EA": {
            "name": "Spain",
            "dxcc": 281,
            "country_code": "es",
            "continent": "EU",
            "cq_zones": "14",
        }
    }

    app.inject_prefixes_into_redis(prefixes)

    # One hset call, with key rcldx:prefix:EA
    pipe = fake_client.pipeline_obj
    assert pipe.executed >= 1
    assert len(pipe.hset_calls) == 1
    call = pipe.hset_calls[0]
    assert call["key"] == "rcldx:prefix:EA"
    mapping = call["mapping"]
    assert mapping["name"] == "Spain"
    # Stored as strings
    assert mapping["dxcc"] == "281"
    assert mapping["continent"] == "EU"


# ---------------------------------------------------------------------------
# clean_redis_prefixes
# ---------------------------------------------------------------------------


def test_clean_redis_prefixes_no_keys(monkeypatch, capsys):
    """clean_redis_prefixes should be graceful if no keys match."""
    fake_client = FakeRedisClient(keys=[])

    def fake_get_client():
        return fake_client

    monkeypatch.setattr(app, "_get_redis_client", fake_get_client)

    app.clean_redis_prefixes()
    captured = capsys.readouterr()
    assert "No keys matching 'rcldx:prefix:*' found" in captured.out


def test_clean_redis_prefixes_deletes_keys(monkeypatch):
    """clean_redis_prefixes should delete keys matching the pattern."""
    keys = ["rcldx:prefix:EA", "rcldx:prefix:EA1", "other:key"]
    fake_client = FakeRedisClient(keys=keys)

    def fake_get_client():
        return fake_client

    monkeypatch.setattr(app, "_get_redis_client", fake_get_client)

    app.clean_redis_prefixes()
    # 'other:key' should remain
    assert "rcldx:prefix:EA" in fake_client.deleted
    assert "rcldx:prefix:EA1" in fake_client.deleted
    assert "other:key" not in fake_client.deleted


# ---------------------------------------------------------------------------
# CLI / main()
# ---------------------------------------------------------------------------


def test_main_no_args_prints_help(capsys):
    """main() with no args should print help."""
    app.main([])
    captured = capsys.readouterr()
    assert "usage:" in captured.out


def test_main_parsecsv_creates_json(tmp_path: Path, monkeypatch):
    """--parsecsv should read CSV and create JSON at DEFAULT_JSON_PATH."""
    # Small CSV
    csv_content = (
        "\ufeffPrefix,Short Name,ADIF DXCC Code,Country Code,Continent,CQ Zones,Likely Prefixes\n"
        'EA,Spain,281,es,EU,14,"EA1, EA2"\n'
    )
    csv_path = tmp_path / "prefixes.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    json_path = tmp_path / "prefixes.json"

    monkeypatch.setattr(app, "CSV_PATH", csv_path)
    monkeypatch.setattr(app, "DEFAULT_JSON_PATH", json_path)

    app.main(["--parsecsv"])

    assert json_path.exists()
    data = json.loads(json_path.read_text(encoding="utf-8"))
    # Should contain EA, EA1, EA2
    assert "EA" in data
    assert "EA1" in data
    assert "EA2" in data


def test_main_injectredis_calls_inject(tmp_path: Path, monkeypatch):
    """--injectredis should load JSON and pass it to inject_prefixes_into_redis."""
    json_path = tmp_path / "prefixes.json"
    payload = {
        "EA": {
            "name": "Spain",
            "dxcc": 281,
            "country_code": "es",
            "continent": "EU",
            "cq_zones": "14",
        }
    }
    json_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(app, "DEFAULT_JSON_PATH", json_path)

    received: Dict[str, Dict[str, Any]] = {}

    def fake_inject(prefixes: Dict[str, Dict[str, Any]]):
        received.update(prefixes)

    monkeypatch.setattr(app, "inject_prefixes_into_redis", fake_inject)

    app.main(["--injectredis"])

    assert "EA" in received
    assert received["EA"]["dxcc"] == 281


def test_main_clean_local_and_redis_order(monkeypatch):
    """
    --clean all should call clean_local_files() then clean_redis_prefixes().
    """
    calls: List[str] = []

    def fake_clean_local():
        calls.append("local")

    def fake_clean_redis():
        calls.append("redis")

    monkeypatch.setattr(app, "clean_local_files", fake_clean_local)
    monkeypatch.setattr(app, "clean_redis_prefixes", fake_clean_redis)

    app.main(["--clean", "all"])

    assert calls == ["local", "redis"]


def test_main_clean_local_only(monkeypatch):
    """--clean local should only clean local files."""
    calls: List[str] = []

    def fake_clean_local():
        calls.append("local")

    def fake_clean_redis():
        calls.append("redis")

    monkeypatch.setattr(app, "clean_local_files", fake_clean_local)
    monkeypatch.setattr(app, "clean_redis_prefixes", fake_clean_redis)

    app.main(["--clean", "local"])
    assert calls == ["local"]


def test_main_clean_redis_only(monkeypatch):
    """--clean redis should only clean redis keys."""
    calls: List[str] = []

    def fake_clean_local():
        calls.append("local")

    def fake_clean_redis():
        calls.append("redis")

    monkeypatch.setattr(app, "clean_local_files", fake_clean_local)
    monkeypatch.setattr(app, "clean_redis_prefixes", fake_clean_redis)

    app.main(["--clean", "redis"])
    assert calls == ["redis"]
