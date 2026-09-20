#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: perf/benchmarks/run.sh [options]

Run the Go microbenchmarks with reproducible profiles and captured evidence.

Profiles:
  smoke           Fast local check: 1s, 3 samples, 1 process run
  standard        Development comparison: 5s, 5 samples, 1 process run
  qualification  Release evidence: 10s, 10 samples, 3 process runs

Options:
  -p, --profile NAME       Benchmark profile (default: standard)
      --suite NAME         Benchmark suite: all, append, or fetch (default: all)
      --bench REGEX        Override the benchmark selection regular expression
      --benchtime VALUE     Override Go benchmark duration
      --count VALUE         Override Go benchmark sample count
      --runs VALUE          Override independent process runs
      --cpu VALUE           Go benchmark CPU list (default: 1)
      --timeout VALUE       Go test timeout (profile default varies)
      --run-dir PATH        Evidence directory (default: $HOME/immulog-benchmark-TIMESTAMP)
      --allow-dirty         Permit a dirty worktree (qualification rejects it by default)
      --no-benchmem         Do not include allocation measurements
  -h, --help, help          Show this help

Each run writes raw benchmark output, a command file, an environment capture,
a summary, and the effective configuration under the evidence directory.
EOF
}

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

profile=standard
suite=all
bench_override=''
benchtime_override=''
count_override=''
runs_override=''
cpu_override=''
timeout_override=''
run_root="$HOME/immulog-benchmark-$(date +%Y%m%d-%H%M%S)"
allow_dirty=0
benchmem=1

require_value() {
	if [[ $# -lt 2 || -z "$2" ]]; then
		echo "missing value for $1" >&2
		usage >&2
		exit 2
	fi
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	-p|--profile)
		require_value "$@"
		profile=$2
		shift 2
		;;
	--profile=*) profile=${1#*=}; shift ;;
	--suite)
		require_value "$@"
		suite=$2
		shift 2
		;;
	--suite=*) suite=${1#*=}; shift ;;
	--bench)
		require_value "$@"
		bench_override=$2
		shift 2
		;;
	--bench=*) bench_override=${1#*=}; shift ;;
	--benchtime)
		require_value "$@"
		benchtime_override=$2
		shift 2
		;;
	--benchtime=*) benchtime_override=${1#*=}; shift ;;
	--count)
		require_value "$@"
		count_override=$2
		shift 2
		;;
	--count=*) count_override=${1#*=}; shift ;;
	--runs)
		require_value "$@"
		runs_override=$2
		shift 2
		;;
	--runs=*) runs_override=${1#*=}; shift ;;
	--cpu)
		require_value "$@"
		cpu_override=$2
		shift 2
		;;
	--cpu=*) cpu_override=${1#*=}; shift ;;
	--timeout)
		require_value "$@"
		timeout_override=$2
		shift 2
		;;
	--timeout=*) timeout_override=${1#*=}; shift ;;
	-R|--run-dir)
		require_value "$@"
		run_root=$2
		shift 2
		;;
	--run-dir=*) run_root=${1#*=}; shift ;;
	--allow-dirty) allow_dirty=1; shift ;;
	--no-benchmem) benchmem=0; shift ;;
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

case "$profile" in
smoke)
	benchtime=1s
	count=3
	runs=1
	timeout=5m
	;;
standard)
	benchtime=5s
	count=5
	runs=1
	timeout=15m
	;;
qualification)
	benchtime=10s
	count=10
	runs=3
	timeout=30m
	;;
*)
	echo "profile must be smoke, standard, or qualification" >&2
	exit 2
	;;
esac

case "$suite" in
all) bench='.' ;;
append) bench='^Benchmark(IngressAppend|IngressAppendParallel|IngressBatchLinger|DirectAppendBatch)' ;;
fetch) bench='^Benchmark(Fetch|FetchParallel)' ;;
*)
	echo "suite must be all, append, or fetch" >&2
	exit 2
	;;
esac

[[ -n "$bench_override" ]] && bench=$bench_override
[[ -n "$benchtime_override" ]] && benchtime=$benchtime_override
[[ -n "$count_override" ]] && count=$count_override
[[ -n "$runs_override" ]] && runs=$runs_override
[[ -n "$cpu_override" ]] && cpu=$cpu_override || cpu=1
[[ -n "$timeout_override" ]] && timeout=$timeout_override

if ! [[ "$count" =~ ^[1-9][0-9]*$ ]]; then
	echo "count must be a positive integer" >&2
	exit 2
fi
if ! [[ "$runs" =~ ^[1-9][0-9]*$ ]]; then
	echo "runs must be a positive integer" >&2
	exit 2
fi
if [[ -e "$run_root" ]] && [[ -n "$(find "$run_root" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
	echo "run directory is not empty: $run_root" >&2
	exit 2
fi
if (( allow_dirty == 0 )) && [[ "$profile" == qualification ]] && [[ -n "$(git status --porcelain)" ]]; then
	echo "qualification requires a clean worktree; use --allow-dirty to override" >&2
	exit 2
fi

mkdir -p "$run_root"

command_file="$run_root/command.txt"
environment_file="$run_root/environment.txt"
configuration_file="$run_root/configuration.txt"
summary_file="$run_root/summary.tsv"

cat > "$configuration_file" <<EOF
profile=$profile
suite=$suite
bench=$bench
benchtime=$benchtime
count=$count
runs=$runs
cpu=$cpu
timeout=$timeout
benchmem=$benchmem
allow_dirty=$allow_dirty
commit=$(git rev-parse HEAD)
EOF

{
	printf 'date=%s\n' "$(date --iso-8601=seconds)"
	printf 'repository=%s\n' "$repo_root"
	printf 'commit=%s\n' "$(git rev-parse HEAD)"
	printf 'branch=%s\n' "$(git branch --show-current)"
	printf 'worktree_status<<EOF\n%s\nEOF\n' "$(git status --porcelain)"
	printf '\ngo_version:\n'
	go version
	printf '\ngo_env:\n'
	go env GOOS GOARCH GOAMD64 GOMAXPROCS
	printf '\nhost:\n'
	uname -a
	printf '\ncpu_model:\n'
	grep -m1 'model name' /proc/cpuinfo || true
	printf '\nlogical_cpus:\n'
	nproc || true
	printf '\nfilesystem:\n'
	findmnt -T . || true
	df -T . || true
} > "$environment_file"

: > "$command_file"
printf 'run\tstatus\toutput\n' > "$summary_file"

status=0
for ((run=1; run<=runs; run++)); do
	output_file="$run_root/run-$run.txt"
	args=(go test ./perf/benchmarks -run '^$' -bench "$bench" -benchtime="$benchtime" -count="$count" -cpu "$cpu" -timeout "$timeout")
	if (( benchmem )); then
		args+=(-benchmem)
	fi
	printf 'run-%d: ' "$run" >> "$command_file"
	printf '%q ' "${args[@]}" >> "$command_file"
	printf '\n' >> "$command_file"

	printf '\n=== microbenchmark run %d/%d ===\n' "$run" "$runs"
	if "${args[@]}" 2>&1 | tee "$output_file"; then
		run_status=0
	else
		run_status=${PIPESTATUS[0]}
	fi
	printf '%d\t%d\t%s\n' "$run" "$run_status" "$output_file" >> "$summary_file"
	if (( run_status != 0 )); then
		status=$run_status
		break
	fi
done

printf '\nArtifacts: %s\n' "$run_root"
printf 'Summary:   %s\n' "$summary_file"
exit "$status"
