#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BW_DIR="${CERISE_BW_DIR:-/tmp/brightway}"
PROJECT_NAME="${CERISE_FIXED_PROJECT:-render-seed}"
PORT_VALUE="${PORT:-10000}"
SEED_URL_RAW="${CERISE_SEED_URL:-}"
SEED_URL="$(printf '%s' "${SEED_URL_RAW}" | tr -d '\r' | xargs)"

export BRIGHTWAY2_DIR="${BW_DIR}"
export BW2_DIR="${BW_DIR}"

SEED_READY="${BW_DIR}/.seed.ready"
SEED_TAR_GZ="${ROOT_DIR}/Installer/brightway_seed.tar.gz"
SEED_TAR_ZST="${ROOT_DIR}/Installer/brightway_seed.tar.zst"
SEED_DL="${ROOT_DIR}/Installer/.seed_download.tar"
SEED_OK=0

has_project_dirs() {
  local pattern="${BW_DIR}/${PROJECT_NAME}."*
  shopt -s nullglob
  local arr=( $pattern )
  shopt -u nullglob
  [[ ${#arr[@]} -gt 0 ]]
}

if [[ -f "${SEED_READY}" ]]; then
  if [[ -f "${BW_DIR}/projects.db" ]] && has_project_dirs; then
    SEED_OK=1
  else
    echo "[render-start] Seed marker exists but project files are incomplete; rebuilding runtime."
    rm -f "${SEED_READY}"
  fi
fi

if [[ ! -f "${SEED_READY}" ]]; then
  echo "[render-start] Preparing Brightway runtime in ${BW_DIR}"
  # Only wipe/rebuild when we actually have a seed source to extract.
  # In bootstrap mode (no seed), preserve existing runtime data to avoid
  # repeated heavy re-imports after service restarts.
  if [[ -f "${SEED_TAR_GZ}" ]]; then
    rm -rf "${BW_DIR}"
    mkdir -p "${BW_DIR}"
    echo "[render-start] Extracting seed archive: ${SEED_TAR_GZ}"
    tar -xzf "${SEED_TAR_GZ}" -C "${BW_DIR}"
    SEED_OK=1
  elif [[ -f "${SEED_TAR_ZST}" ]]; then
    rm -rf "${BW_DIR}"
    mkdir -p "${BW_DIR}"
    echo "[render-start] Extracting seed archive: ${SEED_TAR_ZST}"
    tar --zstd -xf "${SEED_TAR_ZST}" -C "${BW_DIR}"
    SEED_OK=1
  elif [[ -n "${SEED_URL}" ]]; then
    echo "[render-start] Downloading seed archive from CERISE_SEED_URL"
    if [[ "${SEED_URL}" != http://* && "${SEED_URL}" != https://* ]]; then
      echo "[render-start] WARNING: CERISE_SEED_URL is set but invalid: '${SEED_URL}'"
      echo "[render-start] Falling back to bootstrap mode."
    else
      rm -rf "${BW_DIR}"
      mkdir -p "${BW_DIR}"
      rm -f "${SEED_DL}"
      if curl -fL --retry 3 --retry-delay 2 "${SEED_URL}" -o "${SEED_DL}"; then
        echo "[render-start] Extracting downloaded seed archive"
        if [[ "${SEED_URL}" == *.tar.gz || "${SEED_URL}" == *.tgz ]]; then
          tar -xzf "${SEED_DL}" -C "${BW_DIR}"
        elif [[ "${SEED_URL}" == *.tar.zst || "${SEED_URL}" == *.tzst ]]; then
          tar --zstd -xf "${SEED_DL}" -C "${BW_DIR}"
        else
          # Fallback: let tar autodetect when possible
          tar -xf "${SEED_DL}" -C "${BW_DIR}"
        fi
        SEED_OK=1
      else
        echo "[render-start] WARNING: failed to download seed archive from CERISE_SEED_URL"
        echo "[render-start] Falling back to bootstrap mode."
      fi
      rm -f "${SEED_DL}"
    fi
  else
    mkdir -p "${BW_DIR}"
    echo "[render-start] WARNING: no seed archive found and CERISE_SEED_URL is empty."
    echo "[render-start] Falling back to bootstrap mode (project can be created/imported from UI)."
    if [[ -f "${BW_DIR}/projects.db" ]]; then
      echo "[render-start] Reusing existing bootstrap Brightway data at ${BW_DIR}"
    else
      echo "[render-start] No existing bootstrap data found; first setup will import databases."
    fi
  fi

  if [[ "${SEED_OK}" = "1" ]]; then
    touch "${SEED_READY}"
  fi
fi

if [[ "${SEED_OK}" = "1" ]]; then
  export CERISE_FIXED_PROJECT="${PROJECT_NAME}"
  export CERISE_DISABLE_BOOTSTRAP=1
  echo "[render-start] Fixed project mode: ON (${CERISE_FIXED_PROJECT})"
else
  unset CERISE_FIXED_PROJECT || true
  export CERISE_DISABLE_BOOTSTRAP=0
  echo "[render-start] Fixed project mode: OFF (no seed available)"
fi

echo "[render-start] Brightway dir: ${BW2_DIR}"
echo "[render-start] Port: ${PORT_VALUE}"

exec python "${ROOT_DIR}/DPP_Data Structure/dpp_impact_calculator_online.py" \
  --host 0.0.0.0 \
  --port "${PORT_VALUE}" \
  --bw-dir "${BW_DIR}" \
  --no-browser
