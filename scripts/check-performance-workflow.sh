#!/usr/bin/env bash
set -euo pipefail

workflow=.github/workflows/performance.yml
validator=scripts/validate-performance-inputs.sh

if awk '
/^[[:space:]]+run:[[:space:]]*\|[[:space:]]*$/ { in_run = 1; next }
in_run && /^[[:space:]]+- name:/ { in_run = 0 }
in_run && /\$\{\{[[:space:]]*inputs\./ { print "direct input interpolation in run block: " $0 > "/dev/stderr"; bad = 1 }
END { exit bad }
' "$workflow"; then
	:
else
	exit 1
fi

for variable in BENCHTIME BENCHMARK_COUNT RUN_SOAK_SMOKE SOAK_PROFILE SOAK_DURATION SOAK_PRODUCER_RATE SOAK_ANALYZE; do
	if ! grep -q "^[[:space:]]*$variable: \${{ inputs\." "$workflow"; then
		echo "workflow does not pass $variable through the environment" >&2
		exit 1
	fi
done

env \
	BENCHTIME=1s \
	BENCHMARK_COUNT=5 \
	RUN_SOAK_SMOKE=false \
	SOAK_PROFILE=mixed \
	SOAK_DURATION=20s \
	SOAK_PRODUCER_RATE=0 \
	SOAK_ANALYZE=true \
	bash "$validator"

marker=$(mktemp)
trap 'rm -f "$marker"' EXIT
rm -f "$marker"
if env \
	BENCHTIME='1s"; touch '"$marker"' #' \
	BENCHMARK_COUNT=5 \
	RUN_SOAK_SMOKE=false \
	SOAK_PROFILE=mixed \
	SOAK_DURATION=20s \
	SOAK_PRODUCER_RATE=0 \
	SOAK_ANALYZE=true \
	bash "$validator"; then
	echo "shell metacharacters were accepted as a valid benchmark input" >&2
	exit 1
fi
if [[ -e "$marker" ]]; then
	echo "shell metacharacters were executed instead of rejected" >&2
	exit 1
fi
