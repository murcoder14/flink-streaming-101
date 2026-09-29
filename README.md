# Flink Streaming 101

Hands-on lessons for learning Apache Flink's DataStream API, from basic operators to windows, timers, and keyed state. Each lesson is a small, self-contained Flink job with a built-in data generator, so there is no Kafka or other external system to set up.

Built on **Apache Flink 2.3.0** and **Java 17**.

---

## Prerequisites

| Tool | Version | Needed for | Check with |
|---|---|---|---|
| **JDK** | 17 (the build targets `release 17`; newer JDKs also compile it) | everything | `java -version` |
| **Apache Maven** | 3.8+ | building and running | `mvn -version` |
| **Git** | any recent | cloning | `git --version` |
| **Docker** + **Docker Compose v2** | Docker 24+ | *optional*: running lessons on a real Flink cluster with the Web UI | `docker compose version` |
| **curl** and **python3** | any | *optional*: used by `docker-run.sh` | `curl --version`, `python3 --version` |

Also:

- **OS:** Linux or macOS. On Windows, use WSL2, because the helper scripts are bash.
- **Network:** the first build downloads Flink and its dependencies (a few hundred MB) from Maven Central.
- **Ports:** `8081` must be free if you use the Docker cluster.
- **Background knowledge:** comfortable with Java (generics, lambdas). No prior Flink experience is needed.

You do **not** need to install Flink itself. Local runs use Flink's embedded MiniCluster, and the Docker setup pulls the `flink:2.3.0-java17` image.

---

## Quick start

```bash
git clone <this-repo-url> flink-streaming-101
cd flink-streaming-101

# 1. Build and run the tests (downloads dependencies the first time)
mvn clean verify

# 2. Run the first lesson locally
mvn -q -B -Plocal compile exec:exec -Dexec.executable=java \
    -Dexec.args="-cp %classpath org.muralis.flink.launcher.Lesson1A"
```

Within a few seconds you should see a number printed every 15 seconds (about 750): the number of words generated in the last window. Press **Ctrl-C** to stop. The lessons run forever because their sources are unbounded.

---

## The lessons

| Lesson | Topic | What you'll see | Main source |
|---|---|---|---|
| **1A** | Your first job: a source, `map`, a tumbling processing-time window, and `reduce` | total words counted every 15 s | `launcher/Lesson1A.java` |
| **1B** | Word count: `flatMap` to `Tuple2`, `.returns(Types...)`, `keyBy`, and keyed windows | per-word counts every 5 s | `launcher/Lesson1B.java` |
| **1C** | Writing a FLIP-27 source by hand: `Source`, `SplitEnumerator`, `SourceReader` | a stream of city-bus GPS events | `launcher/Lesson1C.java`, `data/source/gps/` |
| **1D** | Basic operators: `filter`, `map`, `flatMap`, `keyBy` on temperature sensors | three labelled output streams: `MAP`, `FLATMAP`, `KEYBY` | `launcher/Lesson1D.java`, `data/source/temperature/` |
| **2A** | Hospital admissions: `aggregate(AggregateFunction, ProcessWindowFunction)`, per-region windows, capacity alerts | regional admission counts and capacity alerts | `launcher/Lesson2A.java`, `data/source/hospital/` |
| **2B** | Admissions **and** discharges: event-time timers, side outputs, `connect` + `KeyedCoProcessFunction`, checkpointing and UIDs | bed-occupancy alerts (NORMAL / HIGH / FULL) | `launcher/Lesson2B.java`, `data/source/hospital/` |

All paths above are under `src/main/java/org/muralis/flink/`.

Suggested order: **1A, 1B, 1D, 1C, 2A, 2B**. Lesson 1C (the hand-written source) makes more sense once you have used the built-in `DataGeneratorSource` in the other Lesson 1 jobs.

---

## Running a lesson locally (no Docker)

```bash
mvn -q -B -Plocal compile exec:exec -Dexec.executable=java \
    -Dexec.args="-cp %classpath org.muralis.flink.launcher.Lesson2B"
```

Replace `Lesson2B` with any lesson class from the table above. The command compiles the project with the `local` Maven profile and starts the lesson in a fresh JVM, inside Flink's embedded MiniCluster. The profile does two things:

- It switches the Flink dependencies from `provided` to `compile` scope, so they're on the classpath.
- It adds log4j, configured by `src/main/resources/log4j2.properties` to show only `ERROR`s, so the lesson's `print()` output isn't drowned in Flink logging.

Lines are prefixed with the parallel subtask that printed them, e.g. `2> ...`, or with a label such as `BED-OCCUPANCY:2> ...`.

### Tuning knobs (environment variables)

Prefix the run command with the variable, e.g. `ADMISSION_WINDOW_SECONDS=10 mvn -q -B -Plocal ...`.

| Variable | Lesson | Effect | Example value |
|---|---|---|---|
| `ADMISSION_WINDOW_SECONDS` | 2A | shrinks the 2-minute window, so you don't wait as long for results | `ADMISSION_WINDOW_SECONDS=10` |
| `ADMISSION_RATE_PER_SECOND` | 2B | admission rate (default 2/s) | `ADMISSION_RATE_PER_SECOND=20` |
| `SENSOR_RATE` | 1D | temperature readings per second | `SENSOR_RATE=5000` |

With the defaults, region S in Lesson 2B takes about 6.5 minutes to reach HIGH occupancy. The Javadoc of `Lesson2B` explains how to speed that up.

### Running from an IDE

Import the project as a Maven project, **activate the `local` profile** (IntelliJ: Maven tool window → Profiles → `local`), and run any `Lesson*.main()`. Without the profile you'll get `ClassNotFoundException`s for Flink classes, because they are `provided` by default. In IntelliJ you can instead tick *"Add dependencies with 'provided' scope to classpath"* in the run configuration.

---

## Studying a lesson in the Flink Web UI

To see the job graph, parallelism, back pressure, checkpoints, and flame graphs, run the lesson on a small Flink cluster in Docker: one JobManager, and one TaskManager with 4 slots.

```bash
./docker-run.sh 2B          # build the jar, start the cluster, submit Lesson2B
```

Then open **http://localhost:8081**.

| Command | What it does |
|---|---|
| `./docker-run.sh <lesson>` | packages `target/flink-streaming-101.jar`, starts the cluster if needed, cancels any running job, and submits the lesson |
| `./docker-run.sh --logs` | follows the lesson's `print()` output (the TaskManager's stdout) |
| `./docker-run.sh --stop` | cancels the running job |
| `./docker-run.sh --down` | stops and removes the cluster, including the checkpoint volume |
| `FRESH=1 ./docker-run.sh <lesson>` | restarts the TaskManager first, so heap and GC graphs start from a clean baseline |

The environment variables from the table above also work here, e.g. `SENSOR_RATE=5000 ./docker-run.sh 1D`.

Cluster settings, from `docker-compose.yml`:

- **Checkpointing every 10 s**, applied cluster-wide, with the `hashmap` state backend. Checkpoints are kept on cancel. See Web UI → job → *Checkpoints*.
- **Flame graphs enabled**. See Web UI → job → click an operator → *FlameGraph*. `docs/FlameGraph.md` explains how to read them.
- `target/` is mounted read-only into the JobManager at `/opt/jobs`.

You can also drive the cluster by hand:

```bash
mvn clean package
docker compose up -d
docker compose exec jobmanager flink run -d -c org.muralis.flink.launcher.Lesson2B /opt/jobs/flink-streaming-101.jar
docker compose exec jobmanager flink list
docker compose exec jobmanager flink cancel <jobId>
docker compose down -v
```

---

## Running the tests

```bash
mvn test
```

The tests in `src/test/java/.../hospital/` use Flink's real operator test harnesses (`ProcessFunctionTestHarnesses`, `KeyedTwoInputStreamOperatorTestHarness`), with no mocks. They drive watermarks and timers by hand, so they are also good examples of how to unit-test your own stateful functions.

---

## Project layout

```
.
├── pom.xml                     Flink 2.3.0, Java 17; `local` profile for running outside a cluster
├── docker-run.sh               run a lesson on the Docker Flink cluster
├── docker-compose.yml          JobManager + TaskManager, checkpointing, flame graphs
├── docs/                       lesson notes, concept write-ups, exercises and assignments
└── src/
    ├── main/java/org/muralis/flink/
    │   ├── launcher/           Lesson1A … Lesson2B (the entry points)
    │   ├── data/model/         POJOs: SensorReading, AdmitEvent, DischargeEvent, BedOccupancyAlert, …
    │   └── data/source/
    │       ├── temperature/    SensorSource (Lesson 1D)
    │       ├── gps/            hand-written FLIP-27 source (Lesson 1C)
    │       └── hospital/       admission generator, analytics, lifecycle simulator, occupancy monitor (Lessons 2A/2B)
    └── test/java/…             operator-harness tests for the hospital pipeline
```

---

## Further reading and practice

In `docs/`:

- **Concept notes:** `FlatMapFunction.md`, `ProcessWindowFunction.md`, `RichAggregateFunction.md`, `TTLAggregateFunction.md`, `JoinFunction.md`, `keyBy_scenarios.md`, `data_skew.md`, `MailBox_Model.md`, `flink-watermarks-simple-analogies.md`, and more.
- **Lesson write-ups:** `lesson_1A.pdf`, `lesson_1B.pdf`, `lesson2a.md`, `discharge_event_plan.md` (the design behind Lesson 2B).
- **Practice:**
  - `exercises.md`: quizzes and hands-on exercises built on these lessons.
  - `assignment.md`: *VitalWatch*, a multi-part ICU-monitoring assignment covering `ProcessFunction`, keyed and broadcast state, and `CheckpointedFunction`.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ClassNotFoundException: org.apache.flink...` when running from the IDE | Activate the `local` Maven profile (see *Running from an IDE*). |
| `ClassNotFoundException: org.muralis.flink.launcher.LessonXY` | Check the class name in `-Dexec.args` against the lessons table (e.g. `Lesson2B`, not `2B`). |
| No output for a long time in 2A | The window is 2 minutes. Use `ADMISSION_WINDOW_SECONDS=10`. |
| No HIGH/FULL alerts yet in 2B | This is expected for the first ~6.5 minutes. See the `Lesson2B` Javadoc. |
| `docker-run.sh` hangs at "waiting for the Flink UI" | Check `docker compose ps` and `docker compose logs jobmanager`, and make sure port 8081 is free. |
| The job on Docker doesn't pick up your code change | `docker-run.sh` re-packages on every run. After a `mvn clean`, it recreates the containers automatically. |
| `UnsupportedClassVersionError` | You're running on a JDK older than 17. |
