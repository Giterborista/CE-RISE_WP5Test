# Render Deployment (No Persistent Disk)

This fork is configured for a stateless Render deployment.

## Required files

- `Installer/brightway_seed.tar.gz` (preferred) or `Installer/brightway_seed.tar.zst`

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

## Runtime behavior

- UI project is fixed by server (`CERISE_FIXED_PROJECT`).
- Calculation path never creates projects or imports BONSAI during requests.
- If the seed is missing/incomplete, setup and calculation will fail fast with a clear error.
