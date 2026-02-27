#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFEST_FILE="${REPO_ROOT}/data/SHA256SUMS.txt"

REPO="uchicago-dss/w26-causal-inference-economics"
TAG="latest"
DEST_ROOT="${REPO_ROOT}"
VERIFY_CHECKSUM=true
OVERWRITE=false
LEGACY_DATA_DIR=""
REQUESTED_ASSETS=()

usage() {
  cat <<'EOF'
Usage:
  scripts/download_data.sh [DATA_DIR]
  scripts/download_data.sh [--tag TAG] [--asset NAME]... [--dest-root DIR] [--overwrite] [--no-verify]

Options:
  --repo OWNER/REPO   GitHub repo slug (default: uchicago-dss/w26-causal-inference-economics)
  --tag TAG           Release tag to pull from (default: latest)
  --asset NAME        Asset name to download (repeatable). Default is all assets in the release.
  --dest-root DIR     Directory used as root for manifest-mapped output paths (default: repo root)
  --overwrite         Replace existing files
  --no-verify         Skip checksum verification
  -h, --help          Show help

Legacy mode:
  Passing DATA_DIR as the first positional argument keeps prior behavior by downloading
  panel_hts10_monthly.csv into DATA_DIR.
EOF
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

manifest_path_for_asset() {
  local asset_name="$1"
  [[ -f "$MANIFEST_FILE" ]] || return 0
  awk -v asset="$asset_name" '
    NF >= 2 {
      path=$2
      n=split(path, parts, "/")
      if (parts[n] == asset) {
        print path
        exit
      }
    }
  ' "$MANIFEST_FILE"
}

manifest_sha_for_asset() {
  local asset_name="$1"
  [[ -f "$MANIFEST_FILE" ]] || return 0
  awk -v asset="$asset_name" '
    NF >= 2 {
      path=$2
      n=split(path, parts, "/")
      if (parts[n] == asset) {
        print $1
        exit
      }
    }
  ' "$MANIFEST_FILE"
}

if [[ $# -gt 0 && "${1#-}" == "$1" ]]; then
  LEGACY_DATA_DIR="$1"
  shift
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      [[ $# -ge 2 ]] || { echo "Missing value for --repo" >&2; exit 1; }
      REPO="$2"
      shift 2
      ;;
    --tag)
      [[ $# -ge 2 ]] || { echo "Missing value for --tag" >&2; exit 1; }
      TAG="$2"
      shift 2
      ;;
    --asset)
      [[ $# -ge 2 ]] || { echo "Missing value for --asset" >&2; exit 1; }
      REQUESTED_ASSETS+=("$2")
      shift 2
      ;;
    --dest-root)
      [[ $# -ge 2 ]] || { echo "Missing value for --dest-root" >&2; exit 1; }
      DEST_ROOT="$2"
      shift 2
      ;;
    --overwrite)
      OVERWRITE=true
      shift
      ;;
    --no-verify)
      VERIFY_CHECKSUM=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$TAG" == "latest" ]]; then
  RELEASE_API="https://api.github.com/repos/${REPO}/releases/latest"
else
  RELEASE_API="https://api.github.com/repos/${REPO}/releases/tags/${TAG}"
fi

if ! RELEASE_JSON="$(curl -fsSL -H "Accept: application/vnd.github+json" "$RELEASE_API")"; then
  echo "Failed to fetch release metadata from $RELEASE_API" >&2
  exit 1
fi

ASSET_COUNT="$(jq '.assets | length' <<<"$RELEASE_JSON")"
if [[ "$ASSET_COUNT" -eq 0 ]]; then
  echo "No assets found in release metadata." >&2
  exit 1
fi

RELEASE_TAG="$(jq -r '.tag_name' <<<"$RELEASE_JSON")"

if [[ ${#REQUESTED_ASSETS[@]} -eq 0 && -n "$LEGACY_DATA_DIR" ]]; then
  REQUESTED_ASSETS=("panel_hts10_monthly.csv")
fi

if [[ ${#REQUESTED_ASSETS[@]} -eq 0 ]]; then
  while IFS= read -r asset_name; do
    REQUESTED_ASSETS+=("$asset_name")
  done < <(jq -r '.assets[].name' <<<"$RELEASE_JSON")
fi

downloaded=0
skipped=0

for asset_name in "${REQUESTED_ASSETS[@]}"; do
  ASSET_JSON="$(jq -rc --arg name "$asset_name" '.assets[] | select(.name == $name)' <<<"$RELEASE_JSON")"
  if [[ -z "$ASSET_JSON" ]]; then
    echo "Asset '$asset_name' not found in release '$RELEASE_TAG'." >&2
    echo "Available assets:" >&2
    jq -r '.assets[].name' <<<"$RELEASE_JSON" >&2
    exit 1
  fi

  DOWNLOAD_URL="$(jq -r '.browser_download_url' <<<"$ASSET_JSON")"
  DIGEST_FIELD="$(jq -r '.digest // empty' <<<"$ASSET_JSON")"
  EXPECTED_SHA=""
  if [[ "$DIGEST_FIELD" == sha256:* ]]; then
    EXPECTED_SHA="${DIGEST_FIELD#sha256:}"
  else
    EXPECTED_SHA="$(manifest_sha_for_asset "$asset_name")"
  fi

  if [[ -n "$LEGACY_DATA_DIR" && "$asset_name" == "panel_hts10_monthly.csv" ]]; then
    OUTPUT_PATH="${LEGACY_DATA_DIR%/}/panel_hts10_monthly.csv"
  else
    RELATIVE_OUTPUT_PATH="$(manifest_path_for_asset "$asset_name")"
    if [[ -z "$RELATIVE_OUTPUT_PATH" ]]; then
      RELATIVE_OUTPUT_PATH="outside_data/${asset_name}"
    fi
    OUTPUT_PATH="${DEST_ROOT%/}/${RELATIVE_OUTPUT_PATH}"
  fi

  mkdir -p "$(dirname "$OUTPUT_PATH")"

  if [[ -f "$OUTPUT_PATH" && "$OVERWRITE" == false ]]; then
    if [[ "$VERIFY_CHECKSUM" == true && -n "$EXPECTED_SHA" ]]; then
      CURRENT_SHA="$(sha256_file "$OUTPUT_PATH")"
      if [[ "$CURRENT_SHA" == "$EXPECTED_SHA" ]]; then
        echo "Skipping ${asset_name}: already present at ${OUTPUT_PATH}"
        skipped=$((skipped + 1))
        continue
      fi
      echo "Re-downloading ${asset_name}: existing checksum mismatch."
    else
      echo "Skipping ${asset_name}: ${OUTPUT_PATH} exists (use --overwrite to replace)."
      skipped=$((skipped + 1))
      continue
    fi
  fi

  TMP_FILE="$(mktemp)"
  if ! curl -L --fail --retry 3 -o "$TMP_FILE" "$DOWNLOAD_URL"; then
    rm -f "$TMP_FILE"
    echo "Failed to download ${asset_name}" >&2
    exit 1
  fi

  if [[ "$VERIFY_CHECKSUM" == true && -n "$EXPECTED_SHA" ]]; then
    ACTUAL_SHA="$(sha256_file "$TMP_FILE")"
    if [[ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]]; then
      echo "Checksum mismatch for ${asset_name}" >&2
      echo "Expected: ${EXPECTED_SHA}" >&2
      echo "Actual:   ${ACTUAL_SHA}" >&2
      rm -f "$TMP_FILE"
      exit 1
    fi
  elif [[ "$VERIFY_CHECKSUM" == true ]]; then
    echo "Warning: no checksum found for ${asset_name}; verification skipped."
  fi

  mv "$TMP_FILE" "$OUTPUT_PATH"
  echo "Saved ${asset_name} -> ${OUTPUT_PATH}"
  downloaded=$((downloaded + 1))
done

echo "Complete: downloaded=${downloaded}, skipped=${skipped}, release=${RELEASE_TAG}"
