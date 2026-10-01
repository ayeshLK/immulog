#!/usr/bin/env bash

parse_duration_seconds() {
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
		total=$(awk -v total="$total" -v number="$number" -v factor="$factor" 'BEGIN { printf "%.0f", total + number * factor }')
	done
	printf '%s\n' "$total"
}

soak_shutdown_grace_seconds() {
	local duration_seconds=$1 grace_seconds
	grace_seconds=$((duration_seconds / 8))
	if (( grace_seconds < 60 )); then
		grace_seconds=60
	elif (( grace_seconds > 1800 )); then
		grace_seconds=1800
	fi
	printf '%s\n' "$grace_seconds"
}

soak_minimum_timeout_seconds() {
	local duration_seconds=$1 warmup_seconds=$2 grace_seconds
	grace_seconds=$(soak_shutdown_grace_seconds "$duration_seconds")
	printf '%s\n' $((duration_seconds + warmup_seconds + grace_seconds))
}
