#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: scripts/qualify-durability.sh [--output-dir PATH]

Run the native durability evidence suite on macOS or Linux and create an
upload-ready .tar.gz archive. No administrator privileges are required.

Options:
  --output-dir PATH  New or empty evidence directory. It should be on the
                     filesystem being evaluated and outside the Git clone. The
                     default is a timestamped directory under the current
                     user's home directory.
  --help             Show this help.

The runner refuses a dirty Git worktree. It uses an isolated temporary
directory beneath the evidence directory and never opens an application data
directory. The tests cover deterministic fault injection and process crashes,
not physical power loss.
EOF
}

require_value() {
	if [[ $# -lt 2 || -z "$2" ]]; then
		echo "missing value for $1" >&2
		usage >&2
		exit 2
	fi
}

output_dir=''
while [[ $# -gt 0 ]]; do
	case "$1" in
		--output-dir) require_value "$@"; output_dir=$2; shift 2 ;;
		--output-dir=*) output_dir=${1#*=}; shift ;;
		--help|-h|help) usage; exit 0 ;;
		*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done

for command in git go tar tee; do
	command -v "$command" >/dev/null 2>&1 || {
		echo "required command is not available: $command" >&2
		exit 2
	}
done

platform=$(uname -s)
case "$platform" in
	Darwin|Linux) ;;
	*) echo "this runner supports macOS and Linux; detected: $platform" >&2; exit 2 ;;
esac

repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || {
	echo "run this script from an immulog clone" >&2
	exit 2
}
cd "$repo_root"

if [[ -n "$(git status --porcelain)" ]]; then
	echo "the worktree must be clean before qualification" >&2
	git status --short >&2
	exit 2
fi

go_version=$(go env GOVERSION)
if [[ ! "$go_version" =~ ^go1\.(25|2[6-9]|[3-9][0-9])([.].*)?$ ]]; then
	echo "Go 1.25 or newer is required; detected: $go_version" >&2
	exit 2
fi

if [[ -z "$output_dir" ]]; then
	output_dir="$HOME/immulog-qualification-$(date -u +%Y%m%d-%H%M%S)"
fi
if [[ -e "$output_dir" ]]; then
	output_dir=$(cd "$output_dir" && pwd -P)
else
	output_parent=$(dirname "$output_dir")
	output_name=$(basename "$output_dir")
	if [[ ! -d "$output_parent" ]]; then
		echo "output directory parent does not exist: $output_parent" >&2
		exit 2
	fi
	if [[ "$output_name" == . || "$output_name" == .. ]]; then
		echo "output directory must name a new or empty directory" >&2
		exit 2
	fi
	output_parent=$(cd "$output_parent" && pwd -P)
	output_dir="$output_parent/$output_name"
fi
case "$output_dir" in
	"$repo_root"|"$repo_root"/*)
		echo "output directory must be outside the Git clone: $output_dir" >&2
		exit 2
		;;
esac
if [[ -e "$output_dir" ]] && [[ -n "$(find "$output_dir" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
	echo "output directory is not empty: $output_dir" >&2
	exit 2
fi
mkdir -p "$output_dir"
test_tmp="$output_dir/test-tmp"
mkdir -p "$test_tmp"
export TMPDIR="$test_tmp"
export TMP="$test_tmp"
export TEMP="$test_tmp"

environment_file="$output_dir/environment.txt"
commands_file="$output_dir/commands.txt"
summary_file="$output_dir/summary.tsv"
printf 'step\tstatus\tlog\n' > "$summary_file"
: > "$commands_file"

{
	printf 'captured_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf 'repository=%s\n' "$repo_root"
	printf 'commit=%s\n' "$(git rev-parse HEAD)"
	printf 'branch=%s\n' "$(git branch --show-current)"
	printf 'worktree_status=clean\n'
	printf 'evidence_directory=%s\n' "$output_dir"
	printf 'test_temporary_directory=%s\n' "$test_tmp"
	printf '\ngo_version:\n'; go version
	printf '\ngo_environment:\n'; go env GOOS GOARCH CGO_ENABLED
	printf '\nhost:\n'; uname -a
	printf '\nfilesystem:\n'; df -h "$test_tmp"
	if [[ "$platform" == Darwin ]]; then
		printf '\nmacos_version:\n'; sw_vers 2>&1 || true
		printf '\nvolume:\n'
		volume=$(df "$test_tmp" | awk 'END {print $1}')
		diskutil info "$volume" 2>&1 || true
	else
		printf '\nlinux_release:\n'; sed -n '1,80p' /etc/os-release 2>&1 || true
		printf '\nmount:\n'; findmnt -T "$test_tmp" 2>&1 || true
		df -T "$test_tmp" 2>&1 || true
	fi
} > "$environment_file" 2>&1

failed=0
run_step() {
	local name=$1
	shift
	local log_file="$output_dir/$name.log"
	printf '%s: ' "$name" >> "$commands_file"
	printf '%q ' "$@" >> "$commands_file"
	printf '\n' >> "$commands_file"
	printf '\n=== %s ===\n' "$name"
	set +e
	"$@" 2>&1 | tee "$log_file"
	local status=${PIPESTATUS[0]}
	set -e
	printf '%s\t%d\t%s\n' "$name" "$status" "$(basename "$log_file")" >> "$summary_file"
	if (( status != 0 )); then
		failed=1
	fi
}

run_step storage-shuffled go test ./storage -shuffle=on -count=1
run_step storage-race go test -race ./storage -count=1
run_step crash-recovery go test ./storage -run 'Test(ProcessCrashRecovery|PersistenceBoundaryCrashRecovery)$' -count=10 -v
run_step all-packages go test ./... -count=1
run_step vet go vet ./...

printf 'completed_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$environment_file"
archive_path="${output_dir%/}.tar.gz"
tar -czf "$archive_path" -C "$(dirname "$output_dir")" "$(basename "$output_dir")"
if command -v shasum >/dev/null 2>&1; then
	shasum -a 256 "$archive_path" > "$archive_path.sha256"
elif command -v sha256sum >/dev/null 2>&1; then
	sha256sum "$archive_path" > "$archive_path.sha256"
fi

printf '\nEvidence directory: %s\n' "$output_dir"
printf 'Upload-ready archive: %s\n' "$archive_path"
printf 'Review the archive for private information before attaching it to issue #67.\n'
if (( failed != 0 )); then
	echo 'Qualification completed with one or more failures; preserve the archive and report the first failure.' >&2
	exit 1
fi
echo 'Qualification commands passed. Maintainer review is still required before changing platform claims.'
