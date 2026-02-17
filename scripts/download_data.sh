#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${1:-transformed_data}"
mkdir -p "$DATA_DIR"

URL="https://github.com/uchicago-dss/w26-causal-inference-economics/releases/download/data-v1/panel_hts10_monthly.csv"

EXPECTED_SHA="e7a7a9b1bccecc0f80b23fdfb396eaaaaa9d4ea3c56ea84192d5216588b348a8"
OUT="$DATA_DIR/panel_hts10_monthly.csv"

TMP="$(mktemp)"
curl -L --fail --retry 3 -o "$TMP" "$URL"

ACTUAL_SHA="$(shasum -a 256 "$TMP" | awk '{print $1}')"
if [[ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]]; then
  echo "Checksum mismatch"
  echo "Expected: $EXPECTED_SHA"
  echo "Actual:   $ACTUAL_SHA"
  rm -f "$TMP"
  exit 1
fi

mv "$TMP" "$OUT"
echo "Saved $OUT"
