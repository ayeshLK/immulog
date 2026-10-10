#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: perf/history/run.sh --run-dir PATH [options]

Create isolated durable histories and write cold-reopen evidence.

Options:
  --run-dir PATH          Evidence directory (required; must not be populated)
  --records LIST          Records per partition, comma-separated (default: 1000,10000,100000)
  --partitions N          Number of data-bearing topic partitions (default: 1)
  --segment-bytes N       Segment limit (default: 65536)
  --batch-records N       Records per append batch (default: 32)
  --catalog-topics N      Total topics, including the data-bearing topic (default: 1)
  --consumer-commits N    Offset commits on partition zero (default: 0; must not exceed records)
  --runs N                Independent repetitions of the matrix (default: 1)
  --snapshots             Add valid- and invalid-snapshot cases
  --help                  Show this help

The runner preserves environment and configuration metadata plus every case's
data directory and report.json. Do not use a user data directory.
EOF
}

require_value() {
	if [[ $# -lt 2 || -z "$2" ]]; then
		echo "missing value for $1" >&2
		usage >&2
		exit 2
	fi
}

run_dir=''
records='1000,10000,100000'
partitions=1
segment_bytes=65536
batch_records=32
catalog_topics=1
consumer_commits=0
runs=1
snapshots=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--run-dir) require_value "$@"; run_dir=$2; shift 2 ;;
		--run-dir=*) run_dir=${1#*=}; shift ;;
		--records) require_value "$@"; records=$2; shift 2 ;;
		--records=*) records=${1#*=}; shift ;;
		--partitions) require_value "$@"; partitions=$2; shift 2 ;;
		--partitions=*) partitions=${1#*=}; shift ;;
		--segment-bytes) require_value "$@"; segment_bytes=$2; shift 2 ;;
		--segment-bytes=*) segment_bytes=${1#*=}; shift ;;
		--batch-records) require_value "$@"; batch_records=$2; shift 2 ;;
		--batch-records=*) batch_records=${1#*=}; shift ;;
		--catalog-topics) require_value "$@"; catalog_topics=$2; shift 2 ;;
		--catalog-topics=*) catalog_topics=${1#*=}; shift ;;
		--consumer-commits) require_value "$@"; consumer_commits=$2; shift 2 ;;
		--consumer-commits=*) consumer_commits=${1#*=}; shift ;;
		--runs) require_value "$@"; runs=$2; shift 2 ;;
		--runs=*) runs=${1#*=}; shift ;;
		--snapshots) snapshots=1; shift ;;
		--help|-h|help) usage; exit 0 ;;
		*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done
[[ -n "$run_dir" ]] || { echo '--run-dir is required' >&2; usage >&2; exit 2; }
for setting in partitions segment_bytes batch_records catalog_topics runs; do
	value=${!setting}
	[[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "invalid ${setting//_/-}: $value" >&2; exit 2; }
done
[[ "$consumer_commits" =~ ^[0-9]+$ ]] || { echo "invalid consumer-commits: $consumer_commits" >&2; exit 2; }
if [[ -e "$run_dir" ]] && [[ -n "$(find "$run_dir" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
	echo "run directory is not empty: $run_dir" >&2
	exit 2
fi
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
mkdir -p "$run_dir"

IFS=',' read -r -a record_cases <<< "$records"
for case_records in "${record_cases[@]}"; do
	[[ "$case_records" =~ ^[1-9][0-9]*$ ]] || { echo "invalid record count: $case_records" >&2; exit 2; }
	if (( consumer_commits > case_records )); then
		echo "consumer-commits ($consumer_commits) exceeds record count ($case_records)" >&2
		exit 2
	fi
done

configuration_file="$run_dir/configuration.txt"
environment_file="$run_dir/environment.txt"
command_file="$run_dir/commands.txt"
summary_file="$run_dir/summary.tsv"

cat > "$configuration_file" <<EOF
records=$records
partitions=$partitions
segment_bytes=$segment_bytes
batch_records=$batch_records
catalog_topics=$catalog_topics
consumer_commits=$consumer_commits
runs=$runs
snapshots=$snapshots
commit=$(git rev-parse HEAD)
EOF

{
	printf 'date=%s\n' "$(date --iso-8601=seconds)"
	printf 'repository=%s\n' "$repo_root"
	printf 'commit=%s\n' "$(git rev-parse HEAD)"
	printf 'branch=%s\n' "$(git branch --show-current)"
	printf 'worktree_status<<EOF\n%s\nEOF\n' "$(git status --porcelain)"
	printf '\ngo_version:\n'; go version
	printf '\ngo_env:\n'; go env GOOS GOARCH GOAMD64 GOMAXPROCS
	printf '\nhost:\n'; uname -a
	printf '\ncpu_model:\n'; grep -m1 'model name' /proc/cpuinfo || true
	printf '\nlogical_cpus:\n'; nproc || true
	printf '\nfilesystem:\n'; findmnt -T "$run_dir" || true
	df -T "$run_dir" || true
} > "$environment_file"

: > "$command_file"
printf 'run\tcase\tstatus\treport\n' > "$summary_file"

run_case() {
	local run=$1
	local name=$2
	shift 2
	local case_dir="$run_dir/run-$run/$name"
	local args=(go run ./perf/history --data-dir "$case_dir/data" --records "$case_records" --partitions "$partitions" --segment-bytes "$segment_bytes" --batch-records "$batch_records" --catalog-topics "$catalog_topics" --consumer-commits "$consumer_commits" --output "$case_dir/report.json" "$@")
	mkdir -p "$case_dir"
	printf 'run-%d/%s: ' "$run" "$name" >> "$command_file"
	printf '%q ' "${args[@]}" >> "$command_file"
	printf '\n' >> "$command_file"
	printf '\n=== history run %d/%d: %s ===\n' "$run" "$runs" "$name"
	local status=0
	"${args[@]}" || status=$?
	printf '%d\t%s\t%d\t%s\n' "$run" "$name" "$status" "$case_dir/report.json" >> "$summary_file"
	return "$status"
}

for ((run=1; run<=runs; run++)); do
	for case_records in "${record_cases[@]}"; do
		run_case "$run" "records-$case_records"
		if (( snapshots )); then
			run_case "$run" "records-$case_records-snapshot" --snapshots
			run_case "$run" "records-$case_records-invalid-snapshot" --snapshots --invalidate-snapshot
		fi
	done
done

printf 'finished_at=%s\n' "$(date --iso-8601=seconds)" >> "$environment_file"
printf 'Artifacts: %s\n' "$run_dir"
