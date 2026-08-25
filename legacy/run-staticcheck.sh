#!/bin/bash
set -euo pipefail

BASELINE=".staticcheck.baseline"
CURRENT=$(mktemp)
CURRENT_NORM=$(mktemp)
BASELINE_NORM=$(mktemp)
trap 'rm -f "$CURRENT" "$CURRENT_NORM" "$BASELINE_NORM"' EXIT

STATICCHECK=$(which staticcheck 2>/dev/null || echo "$HOME/go/bin/staticcheck")

if [ ! -x "$STATICCHECK" ]; then
	echo "staticcheck not found. Install with: go install honnef.co/go/tools/cmd/staticcheck@latest"
	exit 1
fi

echo "Running staticcheck..."

# Run staticcheck and capture output (exit code 1 means issues found, which is expected)
"$STATICCHECK" ./... > "$CURRENT" 2>&1 || true

# Normalize: strip line:col numbers and leading "./" so paths match baseline
sed -E 's/^\.\///; s/:[0-9]+:[0-9]+:/:LINE:COL:/' "$BASELINE" | sort > "$BASELINE_NORM"
sed -E 's/^\.\///; s/:[0-9]+:[0-9]+:/:LINE:COL:/' "$CURRENT" | sort > "$CURRENT_NORM"

# Find issues in current run that are NOT in baseline
NEW_ISSUES=$(comm -23 "$CURRENT_NORM" "$BASELINE_NORM" || true)

if [ -n "$NEW_ISSUES" ]; then
	echo ""
	echo "============================================"
	echo " NEW STATICCHECK ISSUES (not in baseline)"
	echo "============================================"
	echo "$NEW_ISSUES"
	echo ""
	echo "These issues did not exist in the baseline."
	echo "Please fix them or update the baseline with:"
	echo "  staticcheck ./... > .staticcheck.baseline"
	echo ""
	exit 1
fi

echo "No new issues found."
