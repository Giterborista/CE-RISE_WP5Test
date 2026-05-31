# CERISE DPP Workshop

Standalone workshop mockup for a PV panel Digital Product Passport exercise.

This repository is intentionally separate from the main CE-RISE repository. It has no remote configured by default, and it should not be pushed to the CE-RISE Codeberg repository unless you explicitly add a new remote for this workshop project.

## What It Does

- Lets workshop participants build a PV panel DPP inventory row by row from a blank table.
- Keeps participant-entered manufacturer/assembler information even when no impact factor is selected.
- Uses a compact lookup generated from `/Users/bafr/cerise_pef_Python/footprints-v2.1.6.csv`.
- Calculates GWP100 impacts without Brightway.
- Lets participants upload a linked PV cell DPP and multiply its reference impact by the selected number of cells or other reference-output multiplier.
- Exports one complete JSON package with functional unit, inventory, linked DPP information, impact results, and provenance.

## Run Locally

From this folder:

```bash
npm start
```

Open:

```text
http://127.0.0.1:8765/
```

The app is a standalone static workshop site. It does not require Brightway or a local Python server for deployment.

## Deploy On Render

Use the repository root as the Render root directory.

```text
Build Command: npm install
Start Command: npm start
Health Check Path: /
```

Render provides the `PORT` environment variable automatically; `server.mjs` uses it when present.

## Regenerate Footprint Lookup

```bash
python3 scripts/build_footprint_lookup.py \
  --footprints /Users/bafr/cerise_pef_Python/footprints-v2.1.6.csv \
  --classification /Users/bafr/CERISE-SEE/ontology/bonsai/classification.csv \
  --output data/footprint_lookup.json
```

The generated lookup keeps only `metric = GWP100` rows for physical calculation units: `tonnes`, `TJ`, `tonnes (service)`, and `items`.

## Test

```bash
node tests/workshop_calculations.mjs
```
