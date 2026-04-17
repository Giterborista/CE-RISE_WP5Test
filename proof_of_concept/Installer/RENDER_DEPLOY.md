# Render Deployment (No Persistent Disk)

This fork is configured for a stateless Render deployment.

## Seed source options

Choose one:

1. Local file in repo:
   - `Installer/brightway_seed.tar.gz` (preferred) or `Installer/brightway_seed.tar.zst`
2. Remote URL (recommended for large archives):
   - Set `CERISE_SEED_URL` to a direct-download URL for the archive.

The seed archive must contain a ready Brightway directory with:

- the fixed project set in `CERISE_FIXED_PROJECT` (default: `render-seed`)
- BONSAI database
- biosphere database
- EF v3.1 methods

## Render configuration

- Build command:
  - `pip install --no-cache-dir -r Installer/requirements_core_reference.txt && python Installer/install_custom_bw2io.py`
- Start command:
  - `bash Installer/render_start.sh`
- Environment variables:
  - `CERISE_FIXED_PROJECT=render-seed` (or your seeded project name)
  - `CERISE_BW_DIR=/tmp/brightway`
  - `CERISE_DISABLE_BOOTSTRAP=1`
  - `CERISE_SEED_URL=...` (optional, required if no local archive file)

## Runtime behavior

- UI project is fixed by server (`CERISE_FIXED_PROJECT`).
- Calculation path never creates projects or imports BONSAI during requests.
- If the seed is missing/incomplete, setup and calculation will fail fast with a clear error.

## Build a seed archive locally (recommended)

Use a clean Brightway directory with only one project to keep archive size smaller.

1. Prepare clean BW dir:
   - `export BRIGHTWAY2_DIR="$HOME/bw_render_seed"`
   - `export BW2_DIR="$HOME/bw_render_seed"`
   - `rm -rf "$BRIGHTWAY2_DIR" && mkdir -p "$BRIGHTWAY2_DIR"`
2. Create/import the project:
   - run local calculator once
   - project name: `render-seed`
   - click `Refresh methods` and wait until BONSAI + methods are available
3. Archive:
   - `tar -czf brightway_seed.tar.gz -C "$BRIGHTWAY2_DIR" .`
4. Host archive:
   - either copy to `Installer/brightway_seed.tar.gz` (if size allows)
   - or upload externally and set `CERISE_SEED_URL` in Render
