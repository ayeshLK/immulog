#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: perf/soak/run.sh [options]

Run the mixed workload soak with a dedicated data directory and captured evidence.

Options:
  -d, --duration VALUE          Soak duration (default: 4h)
  -p, --profile NAME            Workload profile: mixed or sustained (default: mixed)
  -s, --seed VALUE              Deterministic seed (default: 0x5eed5eed)
  -r, --reopen-interval VALUE   Store reopen interval (default: 10m)
  -a, --append-interval VALUE   Producer append interval (default: 20ms)
  -c, --churn-interval VALUE    Membership replacement interval, or 0 to disable (default: 750ms)
  -t, --timeout VALUE            Go test timeout (default: 4h30m)
  -f, --minimum-free-bytes VALUE Minimum free space or auto (default: auto, ~24 GiB for 4h)
  -n, --minimum-open-files VALUE Minimum open-file limit or auto (default: auto, ~65536 for 4h)
  -R, --run-dir PATH             Evidence directory (default: $HOME/immulog-soak-TIMESTAMP)
  -D, --data-dir PATH            Soak data directory (default: RUN_DIR/data)
  -L, --log-file PATH            Test log (default: RUN_DIR/soak.log)
  -M, --metrics-file PATH         Structured metrics (default: RUN_DIR/metrics.json)
  -E, --environment-file PATH    Environment capture (default: RUN_DIR/environment.txt)
  -h, --help, help               Show this help

The auto resource estimates use conservative rates from the recorded 30-minute
qualification run. Set a numeric value to override an estimate or 0 to disable
the corresponding preflight. The data directory is preserved so its checkpoint
can be inspected or reused for a later resumable run.
EOF
}

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

run_root="$HOME/immulog-soak-$(date +%Y%m%d-%H%M%S)"
data_dir=''
log_file=''
metrics_file=''
environment_file=''
duration=4h
profile=mixed
seed=0x5eed5eed
reopen_interval=10m
append_interval=20ms
churn_interval=750ms
timeout=4h30m
minimum_free_bytes=auto
minimum_open_files=auto

require_value() {
	if [[ $# -lt 2 || -z "$2" ]]; then
		echo "missing value for $1" >&2
		usage >&2
		exit 2
	fi
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	-d|--duration)
		require_value "$@"
		duration=$2
		shift 2
		;;
	--duration=*) duration=${1#*=}; shift ;;
	-p|--profile)
		require_value "$@"
		profile=$2
		shift 2
		;;
	--profile=*) profile=${1#*=}; shift ;;
	-s|--seed)
		require_value "$@"
		seed=$2
		shift 2
		;;
	--seed=*) seed=${1#*=}; shift ;;
	-r|--reopen-interval)
		require_value "$@"
		reopen_interval=$2
		shift 2
		;;
	--reopen-interval=*) reopen_interval=${1#*=}; shift ;;
	-a|--append-interval)
		require_value "$@"
		append_interval=$2
		shift 2
		;;
	--append-interval=*) append_interval=${1#*=}; shift ;;
	-c|--churn-interval)
		require_value "$@"
		churn_interval=$2
		shift 2
		;;
	--churn-interval=*) churn_interval=${1#*=}; shift ;;
	-t|--timeout)
		require_value "$@"
		timeout=$2
		shift 2
		;;
	--timeout=*) timeout=${1#*=}; shift ;;
	-f|--minimum-free-bytes)
		require_value "$@"
		minimum_free_bytes=$2
		shift 2
		;;
	--minimum-free-bytes=*) minimum_free_bytes=${1#*=}; shift ;;
	-n|--minimum-open-files)
		require_value "$@"
		minimum_open_files=$2
		shift 2
		;;
	--minimum-open-files=*) minimum_open_files=${1#*=}; shift ;;
	-R|--run-dir)
		require_value "$@"
		run_root=$2
		shift 2
		;;
	--run-dir=*) run_root=${1#*=}; shift ;;
	-D|--data-dir)
		require_value "$@"
		data_dir=$2
		shift 2
		;;
	--data-dir=*) data_dir=${1#*=}; shift ;;
	-L|--log-file)
		require_value "$@"
		log_file=$2
		shift 2
		;;
	--log-file=*) log_file=${1#*=}; shift ;;
	-M|--metrics-file)
		require_value "$@"
		metrics_file=$2
		shift 2
		;;
	--metrics-file=*) metrics_file=${1#*=}; shift ;;
	-E|--environment-file)
		require_value "$@"
		environment_file=$2
		shift 2
		;;
	--environment-file=*) environment_file=${1#*=}; shift ;;
	-h|--help|help)
		usage
		exit 0
		;;
	--)
		shift
		break
		;;
	*)
		echo "unknown option: $1" >&2
		usage >&2
		exit 2
		;;
	esac
done

if [[ $# -ne 0 ]]; then
	echo "unexpected argument: $1" >&2
	usage >&2
	exit 2
fi

if [[ -z "$data_dir" ]]; then
	data_dir="$run_root/data"
fi
if [[ -z "$log_file" ]]; then
	log_file="$run_root/soak.log"
fi
if [[ -z "$metrics_file" ]]; then
	metrics_file="$run_root/metrics.json"
fi
if [[ -z "$environment_file" ]]; then
	environment_file="$run_root/environment.txt"
fi

if [[ -z "$data_dir" || "$data_dir" == "/" ]]; then
	echo "data directory must be dedicated and cannot be the filesystem root" >&2
	exit 2
fi
if [[ "$minimum_free_bytes" != auto && ! "$minimum_free_bytes" =~ ^[0-9]+$ ]]; then
	echo "minimum free bytes must be auto or a nonnegative integer" >&2
	exit 2
fi
if [[ "$minimum_open_files" != auto && ! "$minimum_open_files" =~ ^[0-9]+$ ]]; then
	echo "minimum open files must be auto or a nonnegative integer" >&2
	exit 2
fi
if [[ "$profile" != mixed && "$profile" != sustained ]]; then
	echo "profile must be mixed or sustained" >&2
	exit 2
fi

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

next_power_of_two() {
	local value=$1 power=1
	while (( power < value )); do
		power=$((power * 2))
	done
	printf '%s\n' "$power"
}

mkdir -p "$data_dir" "$(dirname "$log_file")" "$(dirname "$metrics_file")" "$(dirname "$environment_file")"
free_bytes() {
	df --output=avail -B1 "$1" | tail -n 1 | tr -d ' '
}
data_bytes() {
	du -sb "$1" | awk '{print $1}'
}
open_file_limit=$(ulimit -n)
initial_free_bytes=$(free_bytes "$data_dir")
initial_data_bytes=$(data_bytes "$data_dir")
existing_log_files=$(find "$data_dir" -type f -name '*.log' | wc -l)
duration_seconds=$(parse_duration_seconds "$duration") || {
	echo "duration must use Go duration syntax, got $duration" >&2
	exit 2
}
if (( duration_seconds <= 0 )); then
	echo "duration must be positive, got $duration" >&2
	exit 2
fi
if [[ "$minimum_free_bytes" == auto ]]; then
	growth_bytes=$((duration_seconds * 1572864))
	if (( growth_bytes < 2 * 1024 * 1024 * 1024 )); then
		growth_bytes=$((2 * 1024 * 1024 * 1024))
	fi
	minimum_free_bytes=$(( (growth_bytes + 2 * 1024 * 1024 * 1024 + 1024 * 1024 * 1024 - 1) / (1024 * 1024 * 1024) * (1024 * 1024 * 1024) ))
fi
if [[ "$minimum_open_files" == auto ]]; then
	projected_log_files=$((existing_log_files + (duration_seconds * 5 + 1) / 2 + 2048))
	minimum_open_files=$(next_power_of_two "$projected_log_files")
fi
if ! [[ "$initial_free_bytes" =~ ^[0-9]+$ ]]; then
	echo "could not determine free space for $data_dir" >&2
	exit 2
fi
if (( minimum_free_bytes > 0 && initial_free_bytes < minimum_free_bytes )); then
	echo "insufficient free space: have $initial_free_bytes bytes, require $minimum_free_bytes" >&2
	exit 2
fi
if [[ "$open_file_limit" != unlimited ]] && (( minimum_open_files > 0 && open_file_limit < minimum_open_files )); then
	echo "insufficient open-file limit: have $open_file_limit, require $minimum_open_files" >&2
	exit 2
fi

{
	printf 'started_at=%s\n' "$(date --iso-8601=seconds)"
	printf 'commit=%s\n' "$(git rev-parse HEAD)"
	printf 'branch=%s\n' "$(git branch --show-current)"
	printf 'duration=%s\n' "$duration"
	printf 'profile=%s\n' "$profile"
	printf 'duration_seconds=%s\n' "$duration_seconds"
	printf 'seed=%s\n' "$seed"
	printf 'reopen_interval=%s\n' "$reopen_interval"
	printf 'append_interval=%s\n' "$append_interval"
	printf 'churn_interval=%s\n' "$churn_interval"
	printf 'timeout=%s\n' "$timeout"
	printf 'minimum_free_bytes=%s\n' "$minimum_free_bytes"
	printf 'minimum_open_files=%s\n' "$minimum_open_files"
	printf 'open_file_limit=%s\n' "$open_file_limit"
	printf 'existing_log_files=%s\n' "$existing_log_files"
	printf 'initial_free_bytes=%s\n' "$initial_free_bytes"
	printf 'initial_data_bytes=%s\n' "$initial_data_bytes"
	printf 'data_dir=%s\n' "$data_dir"
	printf 'metrics_file=%s\n' "$metrics_file"
	go version
	go env GOOS GOARCH GOAMD64 GOMAXPROCS
	grep -m1 'model name' /proc/cpuinfo
	nproc
	uname -a
	df -T "$data_dir"
} | tee "$environment_file"

set +e
IMMULOG_SOAK=1 \
IMMULOG_SOAK_DIR="$data_dir" \
IMMULOG_SOAK_PROFILE="$profile" \
IMMULOG_SOAK_SEED="$seed" \
IMMULOG_SOAK_DURATION="$duration" \
IMMULOG_SOAK_REOPEN_INTERVAL="$reopen_interval" \
IMMULOG_SOAK_APPEND_INTERVAL="$append_interval" \
IMMULOG_SOAK_CHURN_INTERVAL="$churn_interval" \
IMMULOG_SOAK_METRICS_FILE="$metrics_file" \
go test -v ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout="$timeout" 2>&1 | tee "$log_file"
status=${PIPESTATUS[0]}
set -e

final_free_bytes=$(free_bytes "$data_dir")
final_data_bytes=$(data_bytes "$data_dir")
printf 'finished_at=%s\nfinal_free_bytes=%s\nfinal_data_bytes=%s\nexit_status=%d\n' \
	"$(date --iso-8601=seconds)" "$final_free_bytes" "$final_data_bytes" "$status" | tee -a "$environment_file"
exit "$status"
