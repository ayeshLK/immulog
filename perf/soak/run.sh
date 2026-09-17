#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: perf/soak/run.sh [options]

Run the mixed workload soak with a dedicated data directory and captured evidence.

Options:
  -d, --duration VALUE          Soak duration (default: 4h)
  -s, --seed VALUE              Deterministic seed (default: 0x5eed5eed)
  -r, --reopen-interval VALUE   Store reopen interval (default: 10m)
  -a, --append-interval VALUE   Producer append interval (default: 20ms)
  -t, --timeout VALUE            Go test timeout (default: 4h30m)
  -R, --run-dir PATH             Evidence directory (default: $HOME/immulog-soak-TIMESTAMP)
  -D, --data-dir PATH            Soak data directory (default: RUN_DIR/data)
  -L, --log-file PATH            Test log (default: RUN_DIR/soak.log)
  -E, --environment-file PATH    Environment capture (default: RUN_DIR/environment.txt)
  -h, --help, help               Show this help

The data directory is preserved so the soak checkpoint can be inspected or
reused for a later resumable run. Run this from the repository checkout.
EOF
}

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

run_root="$HOME/immulog-soak-$(date +%Y%m%d-%H%M%S)"
data_dir=''
log_file=''
environment_file=''
duration=4h
seed=0x5eed5eed
reopen_interval=10m
append_interval=20ms
timeout=4h30m

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
	-t|--timeout)
		require_value "$@"
		timeout=$2
		shift 2
		;;
	--timeout=*) timeout=${1#*=}; shift ;;
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
if [[ -z "$environment_file" ]]; then
	environment_file="$run_root/environment.txt"
fi

if [[ -z "$data_dir" || "$data_dir" == "/" ]]; then
	echo "data directory must be dedicated and cannot be the filesystem root" >&2
	exit 2
fi

mkdir -p "$data_dir" "$(dirname "$log_file")" "$(dirname "$environment_file")"
{
	printf 'started_at=%s\n' "$(date --iso-8601=seconds)"
	printf 'commit=%s\n' "$(git rev-parse HEAD)"
	printf 'branch=%s\n' "$(git branch --show-current)"
	printf 'duration=%s\n' "$duration"
	printf 'seed=%s\n' "$seed"
	printf 'reopen_interval=%s\n' "$reopen_interval"
	printf 'append_interval=%s\n' "$append_interval"
	printf 'timeout=%s\n' "$timeout"
	printf 'data_dir=%s\n' "$data_dir"
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
IMMULOG_SOAK_SEED="$seed" \
IMMULOG_SOAK_DURATION="$duration" \
IMMULOG_SOAK_REOPEN_INTERVAL="$reopen_interval" \
IMMULOG_SOAK_APPEND_INTERVAL="$append_interval" \
go test -v ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout="$timeout" 2>&1 | tee "$log_file"
status=${PIPESTATUS[0]}
set -e

printf 'finished_at=%s\nexit_status=%d\n' "$(date --iso-8601=seconds)" "$status" | tee -a "$environment_file"
exit "$status"
