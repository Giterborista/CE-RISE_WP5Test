# DPP Data Structure Tools (BW2 / BONSAI)

This folder contains the current prototype tooling used to:

1. create/edit a strict DPP JSON from a web form (`dpp_live_form_minimal.py`)
2. run Brightway2-based impact calculations from a DPP JSON (`dpp_impact_calculator_online.py`)

The tools are intended for local use (browser UI served from a local Python process) and rely on the CE-RISE/BONSAI workflow implemented in `cerise_brightway.py`.

## Contents

- `dpp_live_form_minimal.py`
  Local web form for authoring/editing a minimal strict DPP JSON with live preview. The form uses the BONSAI dictionary labels shown to the user, resolves them through a generated BONSAI process index, filters available source locations, and exports strict secondary-flow metadata including `bonsaiProcess`, `sourceLocation`, and `bonsaiCode`.

- `dpp_impact_calculator_online.py`
  Local web calculator that reads a DPP JSON, validates the strict secondary-flow BONSAI contract, builds a strict foreground in Brightway2, and runs LCIA (EF v3.1 methods available in the selected project).

- `cerise_brightway.py`
  Shared Brightway/BONSAI bridge utilities used by the impact calculator.

- `dpp.schema.json`
  JSON Schema used in the DPP workflow.

- `examples/USEME.json`
  Small DPP JSON example for testing the interfaces.

- `examples/dpp_from_black_ink_workbook.json`
  Richer example DPP aligned with the current strict BONSAI-carrying flow format.

## Repository-level dependencies (expected paths)

These scripts use repository-relative paths. The expected layout is:

- `../bonsai_files/`
  BONSAI importer files (top-level repository folder)

- `../dictionaries/dpp_dictionaries.json`
  Strict visible dictionaries consumed by `dpp_live_form_minimal.py`

- `../dictionaries/bonsai_process_index.json`
  Generated BONSAI process index consumed by `dpp_live_form_minimal.py` to map visible process labels to exact BONSAI tokens and allowed locations

- `../dictionaries/build_bonsai_process_index.py`
  Utility script used to regenerate `bonsai_process_index.json` from the live BONSAI database when the BONSAI vocabulary changes

- `../ontology/bonsai/bonsai_flowobject_individuals.ttl`
  Current aligned BONSAI flow-object vocabulary snapshot used as ontology support material for processes, transports, and end-of-life scenarios

- `../Installer/`
  Environment and installation helpers (optional but recommended for reproducibility)

## Environment requirements

Runtime dependencies are managed through the repository `Installer/` folder:

- `../Installer/conda_env_bw_no_builds.yml`
- `../Installer/pip_freeze_bw.txt`
- `../Installer/bw2io_custom_snapshot.zip`
- `../Installer/install_custom_bw2io.py`

At minimum, the environment needs:

- `bw2data`
- `bw2calc`
- `bw2io` (customized build/snapshot recommended)
- `openpyxl`
- `matplotlib`
- `jsonschema`

## Installation (recommended)

From repository root:

```bash
conda env create -f Installer/conda_env_bw_no_builds.yml -n bw
conda activate bw
python Installer/install_custom_bw2io.py
python Installer/check_runtime.py
```

If the environment already exists:

```bash
conda activate bw
python Installer/install_custom_bw2io.py
```

## Tool 1: Minimal DPP JSON Converter

Starts a local web form for creating/editing a strict DPP JSON using the repository dictionaries and the generated BONSAI process index.

From repository root:

```bash
python "DPP_Data Structure/dpp_live_form_minimal.py" \
  --dictionaries "dictionaries/dpp_dictionaries.json" \
  --bonsai-index "dictionaries/bonsai_process_index.json"
```

Expected behavior:

- starts a local HTTP server (default localhost)
- opens (or prints) the local URL
- shows one live JSON panel
- supports JSON upload/edit/download/copy
- shows searchable BONSAI process selectors
- distinguishes visible BONSAI market/activity labels in the UI
- filters source locations according to the selected BONSAI provider
- exports strict secondary-flow data with `bonsaiProcess`, `sourceLocation`, and `bonsaiCode`

## Tool 2: DPP Impact Calculator (Brightway2 + BONSAI)

Starts a local web interface that:

1. loads a DPP JSON (paste or upload)
2. configures/uses a Brightway project
3. imports BONSAI if missing (optional UI choice)
4. validates strict secondary-flow BONSAI metadata (`bonsaiProcess`, `sourceLocation`, `bonsaiCode`)
5. resolves BONSAI providers code-first from `bonsaiCode`
6. runs strict DPP-to-foreground mapping
7. computes LCIA and visualizes results

From repository root:

```bash
python "DPP_Data Structure/dpp_impact_calculator_online.py"
```

Optional custom Brightway directory:

```bash
python "DPP_Data Structure/dpp_impact_calculator_online.py" --bw-dir "/path/to/Brightway3"
```

### First-use checklist (calculator)

In the web UI:

1. Select or create a Brightway project
2. Keep `Import BONSAI if missing` enabled (if project is new)
3. Click `Refresh methods`
4. Upload/paste DPP JSON
5. Select calculation mode and method
6. Run

## Notes on strict behavior

The current implementation intentionally favors traceability over convenience:

- unresolved mappings block calculation
- dictionaries are loaded from a fixed JSON file (no Excel fallback)
- the generator also depends on a generated BONSAI index (`bonsai_process_index.json`)
- secondary flows in the operational workflow must carry `bonsaiProcess`, `sourceLocation`, and `bonsaiCode`
- `bonsaiCode` must be a concrete BONSAI provider code in `<TOKEN>|<LOCATION>` form
- the calculator validates that `bonsaiCode`, `sourceLocation`, and `bonsaiProcess` are internally consistent before calculation
- old operational DPPs without `bonsaiCode` on secondary flows are rejected
- DPP inputs are not silently auto-corrected during impact calculation

This is deliberate for testing and protocol control.

## Troubleshooting

- `BONSAI database 'bonsai' not found`
  Enable BONSAI import in the calculator UI and re-run setup.

- `Biosphere database ... not found`
  Use a new/empty Brightway project and allow the seeding step.

- `BONSAI index file not found`
  Regenerate or restore `dictionaries/bonsai_process_index.json`, then start the generator with `--bonsai-index` pointing to that file.

- `secondary flows require bonsaiCode '<TOKEN>|<LOCATION>'`
  The DPP was created in an older format. Re-export it with the current generator or migrate the secondary flows so each one includes `bonsaiProcess`, `sourceLocation`, and `bonsaiCode`.

- Port already in use
  The tools try the next free local port automatically.

- Custom `bw2io` importer missing BONSAI modules
  Re-run:
  `python Installer/install_custom_bw2io.py`
