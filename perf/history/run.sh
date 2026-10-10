#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: perf/history/run.sh --run-dir PATH [options]

Create one fresh history per case and write JSON reports for cold reopen.

Options:
  --run-dir PATH       Output directory (required; must not be populated)
  --records LIST       Records per partition, comma-separated (default: 1000,10000,100000)
  --partitions N       Number of partitions (default: 1)
  --segment-bytes N    Segment limit (default: 65536)
  --batch-records N    Records per append batch (default: 32)
  --snapshots          Add snapshot-present cases
  --help               Show this help

The runner preserves each case's data directory and report.json. Do not use a
user data directory: this command creates and closes stores in every case.
EOF
}

run_dir=''
records='1000,10000,100000'
partitions=1
segment_bytes=65536
batch_records=32
snapshots=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--run-dir) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; run_dir=$2; shift 2 ;;
		--records) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; records=$2; shift 2 ;;
		--partitions) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; partitions=$2; shift 2 ;;
		--segment-bytes) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; segment_bytes=$2; shift 2 ;;
		--batch-records) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; batch_records=$2; shift 2 ;;
		--snapshots) snapshots=1; shift ;;
		--help|-h) usage; exit 0 ;;
		*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done
[[ -n "$run_dir" ]] || { echo '--run-dir is required' >&2; usage >&2; exit 2; }
if [[ -e "$run_dir" ]] && [[ -n "$(find "$run_dir" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
	echo "run directory is not empty: $run_dir" >&2
	exit 2
fi
mkdir -p "$run_dir"

IFS=',' read -r -a record_cases <<< "$records"
for case_records in "${record_cases[@]}"; do
	[[ "$case_records" =~ ^[1-9][0-9]*$ ]] || { echo "invalid record count: $case_records" >&2; exit 2; }
	case_dir="$run_dir/records-$case_records"
	mkdir -p "$case_dir"
	go run ./perf/history --data-dir "$case_dir/data" --records "$case_records" --partitions "$partitions" --segment-bytes "$segment_bytes" --batch-records "$batch_records" --output "$case_dir/report.json"
	if (( snapshots )); then
		case_dir="$run_dir/records-$case_records-snapshot"
		mkdir -p "$case_dir"
		go run ./perf/history --data-dir "$case_dir/data" --records "$case_records" --partitions "$partitions" --segment-bytes "$segment_bytes" --batch-records "$batch_records" --snapshots --output "$case_dir/report.json"
		case_dir="$run_dir/records-$case_records-invalid-snapshot"
		mkdir -p "$case_dir"
		go run ./perf/history --data-dir "$case_dir/data" --records "$case_records" --partitions "$partitions" --segment-bytes "$segment_bytes" --batch-records "$batch_records" --snapshots --invalidate-snapshot --output "$case_dir/report.json"
	fi
done
printf 'Artifacts: %s\n' "$run_dir"
