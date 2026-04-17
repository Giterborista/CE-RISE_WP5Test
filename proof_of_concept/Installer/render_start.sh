#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BW_DIR="${CERISE_BW_DIR:-/tmp/brightway}"
PROJECT_NAME="${CERISE_FIXED_PROJECT:-render-seed}"
PORT_VALUE="${PORT:-10000}"

export CERISE_FIXED_PROJECT="${PROJECT_NAME}"
export CERISE_DISABLE_BOOTSTRAP=1
export BRIGHTWAY2_DIR="${BW_DIR}"
export BW2_DIR="${BW_DIR}"

SEED_READY="${BW_DIR}/.seed.ready"
SEED_TAR_GZ="${ROOT_DIR}/Installer/brightway_seed.tar.gz"
SEED_TAR_ZST="${ROOT_DIR}/Installer/brightway_seed.tar.zst"

if [[ ! -f "${SEED_READY}" ]]; then
  echo "[render-start] Preparing Brightway runtime in ${BW_DIR}"
  rm -rf "${BW_DIR}"
  mkdir -p "${BW_DIR}"

  if [[ -f "${SEED_TAR_GZ}" ]]; then
    echo "[render-start] Extracting seed archive: ${SEED_TAR_GZ}"
    tar -xzf "${SEED_TAR_GZ}" -C "${BW_DIR}"
  elif [[ -f "${SEED_TAR_ZST}" ]]; then
    echo "[render-start] Extracting seed archive: ${SEED_TAR_ZST}"
    tar --zstd -xf "${SEED_TAR_ZST}" -C "${BW_DIR}"
  else
    echo "[render-start] WARNING: no seed archive found in Installer/. Startup will fail unless project '${PROJECT_NAME}' already exists."
  fi

  touch "${SEED_READY}"
fi

echo "[render-start] Fixed project: ${CERISE_FIXED_PROJECT}"
echo "[render-start] Brightway dir: ${BW2_DIR}"
echo "[render-start] Port: ${PORT_VALUE}"

exec python "${ROOT_DIR}/DPP_Data Structure/dpp_impact_calculator_online.py" \
  --host 0.0.0.0 \
  --port "${PORT_VALUE}" \
  --bw-dir "${BW_DIR}" \
  --no-browser
