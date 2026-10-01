#!/usr/bin/env bash
set -euo pipefail

source perf/soak/timeout.sh

assert_equal() {
	local got=$1 want=$2 description=$3
	if [[ "$got" != "$want" ]]; then
		echo "$description: got $got, want $want" >&2
		exit 1
	fi
}

assert_equal "$(parse_duration_seconds 4h30m)" 16200 "combined duration"
assert_equal "$(soak_shutdown_grace_seconds 20)" 60 "short-run grace"
assert_equal "$(soak_shutdown_grace_seconds 3600)" 450 "one-hour grace"
assert_equal "$(soak_shutdown_grace_seconds 14400)" 1800 "qualification grace"
assert_equal "$(soak_minimum_timeout_seconds 20 0)" 80 "mixed smoke timeout"
assert_equal "$(soak_minimum_timeout_seconds 20 30)" 110 "warm sustained smoke timeout"
assert_equal "$(soak_minimum_timeout_seconds 14400 1800)" 18000 "qualification timeout"

if parse_duration_seconds invalid >/dev/null 2>&1; then
	echo "invalid duration was accepted" >&2
	exit 1
fi
