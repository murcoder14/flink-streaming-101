#!/usr/bin/env bash
# Runs one lesson on your own machine (no Docker, no cluster): ./run.sh <lesson number>
#
#   ./run.sh 1A     basic operators        ./run.sh 4A     timers
#   ./run.sh 2A     aggregations           ./run.sh 5D     side outputs
#   ./run.sh 2B     admits + discharges (timers, connect)
#   ./run.sh 3B     state                  ./run.sh 6A     broadcast state
#
# The lessons never stop by themselves (the sensor stream is endless) - press Ctrl-C when you have seen enough.
# Needs Java 17 and Maven. First run downloads dependencies, so it takes a while.
set -euo pipefail
cd "$(dirname "$0")"

if [[ $# -ne 1 || ! "$1" =~ ^[1-5][A-Z]$ ]]; then
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
fi

lesson_file="src/main/java/org/muralis/flink/launcher/Lesson${1}.java"

if [[ ! -f "$lesson_file" ]]; then
  echo "Error: lesson '$1' does not exist: $lesson_file" >&2
  exit 1
fi

echo "$lesson_file"

lesson_class=$(basename "$lesson_file" .java)

echo "$lesson_class"
echo ">>> Running $lesson_class  (Ctrl-C to stop)"

exec mvn -q -B -Plocal compile exec:exec \
  -Dexec.executable=java \
  -Dexec.args="-cp %classpath org.muralis.flink.launcher.${lesson_class}"
