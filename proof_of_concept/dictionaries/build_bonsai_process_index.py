#!/usr/bin/env python3
"""Build BONSAI process index from the live Brightway BONSAI database.

The generated artifact is used by the strict DPP generator to:
- keep the visible dictionaries unchanged
- filter source locations per selected BONSAI process label
- store exact BONSAI provider codes on secondary flows
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import bw2data as bd
except Exception as exc:  # pragma: no cover
    raise RuntimeError("bw2data is required to build the BONSAI process index") from exc


DEFAULT_DICTIONARIES = Path(__file__).resolve().with_name("dpp_dictionaries.json")
DEFAULT_OUTPUT = Path(__file__).resolve().with_name("bonsai_process_index.json")
DEFAULT_PROJECT = "Bonsai_V3"
DEFAULT_DB = "bonsai"
LABEL_KIND_RE = re.compile(r"\s+\((market for|activity)\)\s*$", flags=re.IGNORECASE)


def _normalize_location(code: str) -> str:
    raw = str(code or "").strip().upper()
    if not raw:
        return ""
    if raw == "GLOBAL":
        return "GLO"
    return raw


def _strip_label_suffix(label: str) -> str:
    return LABEL_KIND_RE.sub("", str(label or "").strip()).strip()


def _kind_from_label(label: str) -> str:
    m = LABEL_KIND_RE.search(str(label or "").strip())
    if not m:
        return ""
    return "market" if str(m.group(1) or "").lower() == "market for" else "activity"


def _load_dictionaries(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data.get("locationMap"), dict):
        raise ValueError("Dictionary file must contain a locationMap object")
    return data


def _representable_location_codes(dicts: dict[str, Any]) -> set[str]:
    codes: set[str] = set()
    for value in dicts.get("locationMap", {}).values():
        norm = _normalize_location(str(value or ""))
        if norm and re.match(r"^[A-Z]{2,3}$", norm):
            codes.add(norm)
    return codes


def _build_name_index(bonsai_db: str) -> dict[str, list[dict[str, str]]]:
    if bonsai_db not in bd.databases:
        raise RuntimeError(f"BONSAI database '{bonsai_db}' not found in current Brightway project")
    out: dict[str, list[dict[str, str]]] = defaultdict(list)
    for ds in bd.Database(bonsai_db):
        code = str(ds["code"])
        if "|" not in code:
            continue
        token, raw_loc = code.split("|", 1)
        record = {
            "code": code,
            "token": token.upper(),
            "location": _normalize_location(raw_loc),
        }
        for field in ("name", "reference product"):
            value = str(ds.get(field, "")).strip()
            if value:
                out[value.lower()].append(record)
    return out


def _resolve_entry(
    label: str,
    records: list[dict[str, str]],
    representable_locations: set[str],
) -> dict[str, Any]:
    provider_kind = _kind_from_label(label)
    if provider_kind == "market":
        candidates = [r for r in records if r["token"].startswith("M_")]
    elif provider_kind == "activity":
        candidates = [r for r in records if not r["token"].startswith("M_")]
    else:
        candidates = list(records)

    codes_by_location: dict[str, set[str]] = defaultdict(set)
    tokens: set[str] = set()
    for record in candidates:
        loc = record["location"]
        if loc not in representable_locations:
            continue
        codes_by_location[loc].add(record["code"])
        tokens.add(record["token"])

    ambiguous_locations = sorted(loc for loc, codes in codes_by_location.items() if len(codes) > 1)
    code_by_location = {
        loc: next(iter(codes))
        for loc, codes in codes_by_location.items()
        if len(codes) == 1
    }

    if not code_by_location:
        raise ValueError(
            f"Label '{label}' has no representable BONSAI locations for provider kind '{provider_kind or 'any'}'"
        )

    allowed_locations = sorted(code_by_location)
    entry: dict[str, Any] = {
        "bonsaiProcess": _strip_label_suffix(label),
        "providerKind": provider_kind or ("market" if any(t.startswith("M_") for t in tokens) else "activity"),
        "allowedLocations": allowed_locations,
        "codeByLocation": {loc: code_by_location[loc] for loc in allowed_locations},
        "bonsaiTokens": sorted(tokens),
    }
    if ambiguous_locations:
        entry["ambiguousLocations"] = ambiguous_locations
    if len(tokens) == 1:
        entry["bonsaiToken"] = next(iter(tokens))
    return entry


def build_index(dicts: dict[str, Any], bonsai_project: str, bonsai_db: str) -> dict[str, Any]:
    bd.projects.set_current(bonsai_project)
    representable_locations = _representable_location_codes(dicts)
    name_index = _build_name_index(bonsai_db)

    labels: list[str] = []
    for key in ("bonsaiProcesses", "transports", "eolScenarios"):
        for label in dicts.get(key, []):
            text = str(label or "").strip()
            if text and text not in labels:
                labels.append(text)

    entries: dict[str, Any] = {}
    missing: list[str] = []
    for label in labels:
        raw = _strip_label_suffix(label).lower()
        records = name_index.get(raw, [])
        if not records:
            missing.append(label)
            continue
        entries[label] = _resolve_entry(label, records, representable_locations)

    if missing:
        preview = ", ".join(missing[:20]) + ("..." if len(missing) > 20 else "")
        raise ValueError(f"Missing BONSAI matches for dictionary labels: {preview}")

    return {
        "metadata": {
            "bonsaiProject": bonsai_project,
            "bonsaiDatabase": bonsai_db,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "entryCount": len(entries),
            "representableLocationCount": len(representable_locations),
            "ambiguousLocationCount": sum(
                len(entry.get("ambiguousLocations", [])) for entry in entries.values()
            ),
        },
        "entries": entries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BONSAI process index for the strict DPP generator")
    parser.add_argument("--dictionaries", type=Path, default=DEFAULT_DICTIONARIES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--bonsai-db", default=DEFAULT_DB)
    args = parser.parse_args()

    dicts = _load_dictionaries(args.dictionaries)
    payload = build_index(dicts, bonsai_project=args.project, bonsai_db=args.bonsai_db)
    with args.output.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(f"Wrote BONSAI process index to {args.output}")


if __name__ == "__main__":
    main()
