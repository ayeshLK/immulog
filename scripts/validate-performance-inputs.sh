#!/usr/bin/env bash
set -euo pipefail

require_env() {
	local name=$1
	if [[ -z "${!name+x}" ]]; then
		echo "missing required environment variable: $name" >&2
		exit 2
	fi
}

for name in BENCHTIME BENCHMARK_COUNT RUN_SOAK_SMOKE SOAK_PROFILE SOAK_DURATION SOAK_PRODUCER_RATE SOAK_ANALYZE; do
	require_env "$name"
done

parse_positive_duration() {
	local remaining=$1 total=0 number unit factor
	while [[ -n "$remaining" ]]; do
		if [[ ! "$remaining" =~ ^([0-9]+([.][0-9]+)?)(ns|us|µs|ms|s|m|h)(.*)$ ]]; then
			return 1
		fi
		number=${BASH_REMATCH[1]}
		unit=${BASH_REMATCH[3]}
		remaining=${BASH_REMATCH[4]}
		case "$unit" in
		ns) factor=0.000000001 ;;
		us|µs) factor=0.000001 ;;
		ms) factor=0.001 ;;
		s) factor=1 ;;
		m) factor=60 ;;
		h) factor=3600 ;;
		esac
		total=$(awk -v total="$total" -v number="$number" -v factor="$factor" 'BEGIN { printf "%.9f", total + number * factor }')
	done
	awk -v total="$total" 'BEGIN { exit !(total > 0) }'
}

if ! parse_positive_duration "$BENCHTIME"; then
	echo "BENCHTIME must be a positive Go duration" >&2
	exit 2
fi
if ! [[ "$BENCHMARK_COUNT" =~ ^[1-9][0-9]*$ ]]; then
	echo "BENCHMARK_COUNT must be a positive integer" >&2
	exit 2
fi
if [[ "$RUN_SOAK_SMOKE" != true && "$RUN_SOAK_SMOKE" != false ]]; then
	echo "RUN_SOAK_SMOKE must be true or false" >&2
	exit 2
fi
if [[ "$SOAK_PROFILE" != mixed && "$SOAK_PROFILE" != sustained ]]; then
	echo "SOAK_PROFILE must be mixed or sustained" >&2
	exit 2
fi
if ! parse_positive_duration "$SOAK_DURATION"; then
	echo "SOAK_DURATION must be a positive Go duration" >&2
	exit 2
fi
if ! [[ "$SOAK_PRODUCER_RATE" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
	echo "SOAK_PRODUCER_RATE must be a nonnegative number" >&2
	exit 2
fi
if [[ "$SOAK_ANALYZE" != true && "$SOAK_ANALYZE" != false ]]; then
	echo "SOAK_ANALYZE must be true or false" >&2
	exit 2
fi

exit 0
