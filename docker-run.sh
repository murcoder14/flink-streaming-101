#!/usr/bin/env bash
# Runs one lesson as a Flink job on the Docker cluster: ./docker-run.sh <lesson number 1-6>
#
# What it does: builds the jar if needed, starts the cluster (Flink UI: http://localhost:8081), cancels whatever job
# is running, and submits the chosen lesson. Run it again with another number to switch lessons.
#   ./docker-run.sh --logs     follow the console output (print()) of the running lesson
#   ./docker-run.sh --stop     cancel the running job
#   ./docker-run.sh --down     stop and remove the whole cluster (including checkpoints)
#
# Optional (environment variables):
#   SENSOR_RATE=5000 ./docker-run.sh 3     readings per second (default: the lesson's own 1-2/s, which is nearly idle)
#   FRESH=1 ./docker-run.sh 3              restart the TaskManager first, so heap/GC graphs start from a clean baseline
set -euo pipefail
cd "$(dirname "$0")"

UI=http://localhost:8081
JAR=flink-streaming-101.jar

running_jobs() {
  curl -s "$UI/jobs/overview" | python3 -c \
    "import sys,json; [print(j['jid']) for j in json.load(sys.stdin)['jobs'] if j['state'] in ('RUNNING','RESTARTING')]"
}
cancel_running() {
  for jid in $(running_jobs); do
    curl -s -X PATCH "$UI/jobs/$jid?mode=cancel" > /dev/null && echo ">>> cancelled job $jid"
  done
}
wait_ready() {  # Flink UI answers and at least one TaskManager has registered
  echo -n ">>> waiting for the Flink UI"
  until curl -s -o /dev/null "$UI/overview"; do echo -n "."; sleep 2; done
  until [[ "$(curl -s "$UI/overview" | python3 -c "import sys,json; print(json.load(sys.stdin)['taskmanagers'])")" -ge 1 ]]; do
    echo -n "."; sleep 2
  done
  echo " ready"
}

case "${1:-}" in
  --logs) exec docker compose logs -f --no-log-prefix --tail 20 taskmanager ;;
  --stop) cancel_running; exit 0 ;;
  --down) exec docker compose down -v ;;
  [0-9]*[A-Z]|[0-9]*) ;;
  *) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac

lesson_file=$(ls src/main/java/org/muralis/flink/launcher/Lesson"$1"*.java 2>/dev/null | head -n 1)
if [[ -z "$lesson_file" ]]; then
  echo "Error: Lesson $1 file not found." >&2
  exit 1
fi
lesson_class=$(basename "$lesson_file" .java)
echo "$lesson_class"

echo ">>> packaging the jar"
mvn -q -B package

docker compose up -d > /dev/null 2>&1
wait_ready

# `mvn clean` deletes and re-creates target/. A container that was started earlier keeps looking at the OLD (deleted)
# directory and would not see the new jar - so if the jar is not visible inside the container, recreate the containers.
if ! docker compose exec -T jobmanager test -f "/opt/jobs/$JAR"; then
  echo ">>> target/ was re-created since the containers started - recreating them"
  docker compose up -d --force-recreate jobmanager taskmanager > /dev/null 2>&1
  wait_ready
elif [[ -n "${FRESH:-}" ]]; then
  echo ">>> restarting the TaskManager (clean heap)"
  cancel_running
  docker compose restart taskmanager > /dev/null 2>&1
  wait_ready
fi

cancel_running
# SENSOR_RATE is read by SensorSource inside the flink client process, hence -e here
docker compose exec -T -e SENSOR_RATE="${SENSOR_RATE:-}" jobmanager flink run -d \
  -c "org.muralis.flink.launcher.${lesson_class}" "/opt/jobs/$JAR" 2>&1 | grep -v "^WARNING"

echo
echo ">>> $lesson_class is running.  Web UI:  $UI"
echo ">>> console output:  ./docker-run.sh --logs"
