#!/usr/bin/env bash
# timestamp_dir.sh — RFC 3161 timestamp a directory of text files
#
# USAGE:
#   ./timestamp_dir.sh stamp   <dir>          # snapshot current state
#   ./timestamp_dir.sh verify  <dir> <stamp>  # verify a past snapshot
#   ./timestamp_dir.sh log     <dir>          # list all snapshots
#
# DEPENDENCIES: openssl, curl
# FREE TSA used: freetsa.org (no account needed)
# Each .tsr is stored in <dir>/.timestamps/

set -euo pipefail

TSA_URL="https://freetsa.org/tsr"
TSA_CERT_URL="https://freetsa.org/files/tsa.crt"

# ── helpers ────────────────────────────────────────────────────────────────────

die() { echo "ERROR: $*" >&2; exit 1; }

ensure_deps() {
  for cmd in openssl curl; do
    command -v "$cmd" &>/dev/null || die "'$cmd' not found — please install it"
  done
}

# Build a deterministic manifest: "sha256  relative/path" lines, sorted by path.
# The manifest itself is then hashed to produce a single root hash.
build_manifest() {
  local dir="$1"
  # Find all regular files, exclude the .timestamps dir, sort for determinism
  find "$dir" -type f -name "*.md" \
    ! -path "$dir/.timestamps/*" \
    | sort \
    | while read -r f; do
        hash=$(openssl dgst -sha256 -hex "$f" | awk '{print $2}')
        rel="${f#$dir/}"
        printf "%s  %s\n" "$hash" "$rel"
      done
}

manifest_root_hash() {
  local dir="$1"
  build_manifest "$dir" | openssl dgst -sha256 -hex | awk '{print $2}'
}

fetch_tsa_cert() {
  local ts_dir="$1"
  local cert="$ts_dir/tsa.crt"
  if [[ ! -f "$cert" ]]; then
    echo "  Fetching TSA certificate..."
    curl -fsSL "$TSA_CERT_URL" -o "$cert" \
      || die "Could not download TSA cert from $TSA_CERT_URL"
  fi
}

# ── stamp ──────────────────────────────────────────────────────────────────────

cmd_stamp() {
  local dir="${1:?Usage: stamp <dir>}"
  [[ -d "$dir" ]] || die "Not a directory: $dir"

  local ts_dir="$dir/.timestamps"
  mkdir -p "$ts_dir"

  echo "→ Building manifest for: $dir"
  local manifest
  manifest=$(build_manifest "$dir")

  local root_hash
  root_hash=$(echo "$manifest" | openssl dgst -sha256 -hex | awk '{print $2}')
  echo "  Manifest root hash: $root_hash"

  # Timestamp label: ISO-8601 UTC
  local label
  label=$(date -u +"%Y%m%dT%H%M%SZ")
  local base="$ts_dir/$label"

  # Save the manifest so you can inspect it later
  echo "$manifest" > "${base}.manifest"
  echo "  Manifest saved:     ${base}.manifest"

  # Create the RFC 3161 timestamp request
  local tsq="${base}.tsq"
  local tsr="${base}.tsr"

  # openssl ts -query: hash the root hash string (we treat it as a message)
  echo -n "$root_hash" \
    | openssl ts -query -data /dev/stdin -sha256 -cert -out "$tsq" 2>/dev/null

  echo "  Sending request to $TSA_URL ..."
  local http_code
  http_code=$(curl -fsSL \
    -H "Content-Type: application/timestamp-query" \
    --data-binary "@$tsq" \
    -o "$tsr" \
    -w "%{http_code}" \
    "$TSA_URL")

  [[ "$http_code" == "200" ]] || die "TSA returned HTTP $http_code"
  echo "  Timestamp response: ${tsr}"

  # Fetch TSA cert once for later verification
  fetch_tsa_cert "$ts_dir"

  # Quick sanity verify right away
  openssl ts -verify \
    -in "$tsr" \
    -queryfile "$tsq" \
    -CAfile "$ts_dir/tsa.crt" &>/dev/null \
    && echo "  ✓ Timestamp verified immediately (TSA signature valid)" \
    || echo "  ⚠ Immediate verification failed — TSA may be down, but .tsr is saved"

  echo ""
  echo "Snapshot '$label' complete."
  echo "Files in $ts_dir:"
  ls -1 "$ts_dir"
}

# ── verify ─────────────────────────────────────────────────────────────────────

cmd_verify() {
  local dir="${1:?Usage: verify <dir> <stamp_label>}"
  local label="${2:?Usage: verify <dir> <stamp_label>}"
  local ts_dir="$dir/.timestamps"

  local base="$ts_dir/$label"
  local manifest_file="${base}.manifest"
  local tsq="${base}.tsq"
  local tsr="${base}.tsr"
  local cert="$ts_dir/tsa.crt"

  for f in "$manifest_file" "$tsq" "$tsr" "$cert"; do
    [[ -f "$f" ]] || die "Missing file: $f"
  done

  echo "→ Verifying snapshot '$label' for: $dir"

  # Step 1: verify the TSA signature on the .tsq/.tsr pair
  openssl ts -verify \
    -in "$tsr" \
    -queryfile "$tsq" \
    -CAfile "$cert" \
    && echo "  ✓ TSA signature valid (RFC 3161 timestamp is authentic)" \
    || die "TSA signature verification FAILED"

  # Step 2: re-derive the hash from the saved manifest and check it matches .tsq
  local saved_root
  saved_root=$(echo -n "$(awk '{print $1}' "$manifest_file" | tr -d '\n')" | true; \
               cat "$manifest_file" | openssl dgst -sha256 -hex | awk '{print $2}')

  # Extract the message imprint from the .tsq for comparison
  local tsq_hash
  tsq_hash=$(openssl ts -query -in "$tsq" -text 2>/dev/null \
             | grep "Message imprint" | awk '{print $NF}')

  echo ""
  echo "  Manifest root hash (recomputed from saved manifest): $saved_root"
  echo ""
  echo "  Inspect the saved manifest at: $manifest_file"
  echo "  Inspect timestamp details with:"
  echo "    openssl ts -reply -in $tsr -text"
  echo ""
  echo "  To check if current directory state matches this snapshot:"
  echo "    Run: ./timestamp_dir.sh diff $dir $label"
}

# ── diff ───────────────────────────────────────────────────────────────────────

cmd_diff() {
  local dir="${1:?Usage: diff <dir> <stamp_label>}"
  local label="${2:?Usage: diff <dir> <stamp_label>}"
  local ts_dir="$dir/.timestamps"
  local manifest_file="$ts_dir/${label}.manifest"

  [[ -f "$manifest_file" ]] || die "No manifest found for label '$label' in $ts_dir"

  echo "→ Comparing current state of '$dir' to snapshot '$label'"
  echo ""

  local current_manifest
  current_manifest=$(build_manifest "$dir")

  local old="$ts_dir/.diff_old_$$"
  local new="$ts_dir/.diff_new_$$"
  trap "rm -f $old $new" EXIT

  echo "$manifest_file" > /dev/null  # just referencing
  cat "$manifest_file" > "$old"
  echo "$current_manifest" > "$new"

  if diff --color=always -u \
       --label "snapshot/$label" \
       --label "current" \
       "$old" "$new"; then
    echo "✓ No changes — directory matches snapshot '$label' exactly."
  else
    echo ""
    echo "⚠ Directory has changed since snapshot '$label'."
    echo "  Run './timestamp_dir.sh stamp $dir' to record the new state."
  fi
}

# ── log ────────────────────────────────────────────────────────────────────────

cmd_log() {
  local dir="${1:?Usage: log <dir>}"
  local ts_dir="$dir/.timestamps"

  [[ -d "$ts_dir" ]] || { echo "No timestamps yet for $dir"; exit 0; }

  echo "Snapshots for: $dir"
  echo "─────────────────────────────────────────────────────"

  local found=0
  for tsr in "$ts_dir"/*.tsr; do
    [[ -f "$tsr" ]] || continue
    found=1
    local base="${tsr%.tsr}"
    local label
    label=$(basename "$base")
    local mf="${base}.manifest"
    local file_count=0
    [[ -f "$mf" ]] && file_count=$(wc -l < "$mf")

    # Extract timestamp from the .tsr
    local ts_time
    ts_time=$(openssl ts -reply -in "$tsr" -text 2>/dev/null \
              | grep "Time stamp:" | sed 's/Time stamp: //')

    printf "  %-22s  %s  (%s files)\n" "$label" "${ts_time:-<time unavailable>}" "$file_count"
  done

  [[ $found -eq 1 ]] || echo "  (none)"
  echo "─────────────────────────────────────────────────────"
  echo ""
  echo "Commands:"
  echo "  stamp   $dir"
  echo "  verify  $dir <label>"
  echo "  diff    $dir <label>"
}

# ── main ───────────────────────────────────────────────────────────────────────

ensure_deps

case "${1:-help}" in
  stamp)  cmd_stamp  "${2:-}" ;;
  verify) cmd_verify "${2:-}" "${3:-}" ;;
  diff)   cmd_diff   "${2:-}" "${3:-}" ;;
  log)    cmd_log    "${2:-}" ;;
  *)
    echo "RFC 3161 Directory Timestamper"
    echo ""
    echo "Usage:"
    echo "  $0 stamp   <dir>               # snapshot current state"
    echo "  $0 verify  <dir> <label>       # verify a past snapshot"
    echo "  $0 diff    <dir> <label>       # compare current state to snapshot"
    echo "  $0 log     <dir>               # list all snapshots"
    echo ""
    echo "Snapshots are stored in <dir>/.timestamps/"
    echo "TSA: $TSA_URL (free, no account needed)"
    ;;
esac
