#!/usr/bin/env python3
"""Build a compact footprint lookup for the PV DPP workshop app."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ALLOWED_UNITS = {"tonnes", "TJ", "tonnes (service)", "items"}
METRIC = "GWP100"


def load_labels(classification_path: Path) -> dict[str, dict[str, str]]:
    labels: dict[str, dict[str, str]] = {}
    if not classification_path.exists():
        return labels
    with classification_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = str(row.get("name") or "").strip()
            if not code or code in labels:
                continue
            desc = str(row.get("description") or "").strip()
            typ = str(row.get("type") or "").strip()
            labels[code] = {"description": desc, "type": typ}
    return labels


def build_lookup(footprints: Path, classification: Path) -> dict[str, Any]:
    if not footprints.exists():
        raise FileNotFoundError(f"Footprint CSV not found: {footprints}")

    labels = load_labels(classification)
    by_code: dict[str, dict[str, Any]] = {}
    regions: set[str] = set()
    row_count = 0
    version = ""

    with footprints.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        required = {"version", "flow_code", "region_code", "unit_reference", "value", "unit_emission", "metric"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Footprint CSV missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            if row["metric"] != METRIC or row["unit_reference"] not in ALLOWED_UNITS:
                continue
            code = row["flow_code"].strip()
            region = row["region_code"].strip()
            unit_ref = row["unit_reference"].strip()
            if not code or not region:
                continue
            try:
                value = float(row["value"])
            except ValueError:
                continue

            version = version or row["version"]
            regions.add(region)
            row_count += 1

            info = labels.get(code, {})
            entry = by_code.setdefault(
                code,
                {
                    "code": code,
                    "label": info.get("description") or code,
                    "description": info.get("description") or "",
                    "type": info.get("type") or "",
                    "units": [],
                    "regionsByUnit": defaultdict(list),
                    "factors": {},
                },
            )
            if unit_ref not in entry["units"]:
                entry["units"].append(unit_ref)
            if region not in entry["regionsByUnit"][unit_ref]:
                entry["regionsByUnit"][unit_ref].append(region)
            entry["factors"][f"{unit_ref}|{region}"] = value

    entries = []
    for entry in by_code.values():
        entry["units"] = sorted(entry["units"], key=lambda u: ["tonnes", "TJ", "tonnes (service)", "items"].index(u))
        entry["regionsByUnit"] = {
            unit: sorted(values, key=lambda r: (r not in {"WE", "Global", "GLO"}, r))
            for unit, values in entry["regionsByUnit"].items()
        }
        entries.append(entry)

    entries.sort(key=lambda e: (e["label"].lower(), e["code"].lower()))
    return {
        "metadata": {
            "name": "CERISE DPP Workshop footprint lookup",
            "sourceCsv": str(footprints),
            "classificationSource": str(classification),
            "sourceVersion": version,
            "metric": METRIC,
            "unitEmission": "tonnes CO2eq",
            "allowedReferenceUnits": sorted(ALLOWED_UNITS),
            "entryCount": len(entries),
            "factorRowCount": row_count,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        },
        "regions": sorted(regions, key=lambda r: (r not in {"WE", "Global", "GLO"}, r)),
        "entries": entries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build compact workshop footprint lookup")
    parser.add_argument("--footprints", type=Path, required=True)
    parser.add_argument("--classification", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("data/footprint_lookup.json"))
    args = parser.parse_args()

    payload = build_lookup(args.footprints.expanduser(), args.classification.expanduser())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {payload['metadata']['entryCount']} entries / {payload['metadata']['factorRowCount']} factors to {args.output}")


if __name__ == "__main__":
    main()
