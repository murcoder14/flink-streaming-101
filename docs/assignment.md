# Assignment: VitalWatch, an ICU Patient-Monitoring Pipeline

Build a streaming pipeline, one assignment at a time, that watches the vital signs of ICU patients and raises alerts for clinicians. Each assignment adds one real requirement from a hospital monitoring team and needs a few new Flink classes to meet it. The assignments get harder as you go. By the end you will have used all 19 classes and interfaces on the list.

Target: **Flink 2.3** (the version in this repo's `pom.xml`). That means `open(OpenContext)` instead of `open(Configuration)`, `Duration` instead of `Time`, and no `SinkFunction`.

> **Hints, not solutions.** Each assignment has hints in collapsible blocks, ordered from gentle to specific. Open them one at a time. No solutions are included. When you want one, ask for it, e.g. *"show me the solution for A4"*.

---

## Class coverage map

| # | Class / interface | First used in | Used again in |
|---|---|---|---|
| 1 | `TypeInformation` | A1 | A3, A5, A6, A8 |
| 2 | `Types` | A1 | every assignment |
| 3 | `OpenContext` | A3 | A4, A5 |
| 4 | `MapFunction` | A1 | A8, A9 |
| 5 | `FlatMapFunction` | A2 | A8 |
| 6 | `MapState` | A5 | A7 |
| 7 | `MapStateDescriptor` | A5 | A6, A7 |
| 8 | `ValueState` | A4 | A5, A7 |
| 9 | `ValueStateDescriptor` | A4 | A5, A7 |
| 10 | `ReadOnlyBroadcastState` | A6 | A7 |
| 11 | `Collector` | A2 | every later assignment |
| 12 | `OutputTag` | A3 | A6, A7 |
| 13 | `ProcessFunction` | A3 | — |
| 14 | `KeyedProcessFunction` | A4 | A5 |
| 15 | `BroadcastProcessFunction` | A6 | — |
| 16 | `KeyedBroadcastProcessFunction` | A7 | — |
| 17 | `CheckpointedFunction` | A8 | — |
| 18 | `FunctionInitializationContext` | A8 | — |
| 19 | `FunctionSnapshotContext` | A8 | — |

The finished pipeline:

```
 device frames (String)
        │
   [A1] MapFunction ─────────────► DeviceFrame
        │
   [A2] FlatMapFunction ─────────► VitalReading (one per measurement)
        │
   [A3] ProcessFunction ─────────► clean readings ──┬── side outputs: invalid / artifact / clock-skew
        │                                            │
        ├─ keyBy(patientId) ─ [A4] KeyedProcessFunction   → device-silent + sustained-tachycardia alerts
        ├─ keyBy(patientId) ─ [A5] KeyedProcessFunction   → NEWS2 early-warning score
        │
 clinical rules (String) ── broadcast ──┐
        ├──────────────── [A6] BroadcastProcessFunction      → threshold alerts (stateless per reading)
        └─ keyBy(patientId) [A7] KeyedBroadcastProcessFunction → sustained-breach alerts (stateful per patient)
                                                 │
                         all alerts ── [A8] FlatMapFunction + CheckpointedFunction → paging batches
                                                 │
                                  [A9] Capstone: one job, UIDs, savepoints, failure drills
```

---

## A0: Setup (no new Flink classes)

Put the code in a new package such as `org.muralis.flink.vitalwatch`, with one launcher per assignment (`VitalWatchA1`, …) in the style of `org.muralis.flink.launcher.Lesson*`.

### 0.1 The raw device feed

Bedside monitors send one pipe-delimited text frame per patient every second:

```
VW1|<deviceId>|<patientId>|<wardId>|<epochMillis>|<measurements>
```

Example:

```
VW1|MON-0712|P-1007|ICU-A|1727500000000|HR=88;SPO2=97;RR=16;SBP=120;DBP=80;TEMP=98.6F;O2=N;CONSC=A
```

| Code | Meaning | Unit / encoding |
|---|---|---|
| `HR` | heart rate | beats/min |
| `SPO2` | oxygen saturation | % |
| `RR` | respiratory rate | breaths/min |
| `SBP` / `DBP` | systolic / diastolic blood pressure | mmHg |
| `TEMP` | temperature | ends in `C` or `F` (vendors differ) |
| `O2` | on supplemental oxygen | `Y` / `N` |
| `CONSC` | consciousness (ACVPU) | `A`, `C`, `V`, `P`, `U` |

A frame may carry only some measurements. Blood pressure, for example, is taken every few minutes, not every second.

### 0.2 Write a generator

Write a `GeneratorFunction<Long, String>` and a `DataGeneratorSource` for it, modeled on `PatientAdmissionSource`. It should produce:

- About 20 patients across wards `ICU-A` and `ICU-B`, rate-limited to about 20 frames/s in total.
- **Scripted scenarios**, so you can see your alerts fire:
  - `P-1007` deteriorates: HR climbs from 85 to 135 and SpO2 falls from 97 to 89 over about 3 minutes.
  - `P-1013`'s device goes silent for 45 s every 5 minutes.
  - `P-1002` has COPD, so an SpO2 of 88–92 is normal for this patient (this matters in A7).
- **Dirty data**: about 2% malformed frames (missing fields, `HR=abc`, a wrong version prefix), about 1% impossible values (`HR=-1`, `SPO2=104`), and about 1% "lead-off" artifacts (`HR=0` together with `SPO2=0`).
- Temperatures in °F from half the devices and °C from the rest.
- Timestamps in event time (epoch millis), about 0.5% of them 10+ minutes in the future (device clock drift).

### 0.3 The clinical-rules feed (first needed in A6)

A second generator emits one rule change about every 20 s from a scripted list (use a slow rate limit, e.g. 0.05/s):

```
RULE|UPSERT|<ruleId>|<wardId or *>|<vitalCode>|<min>|<max>|<sustainSeconds>|<severity>
RULE|DELETE|<ruleId>
CAREPLAN|UPSERT|<patientId>|<vitalCode>|<min>|<max>
CAREPLAN|DELETE|<patientId>|<vitalCode>
```

Example script: start with `R-HR-HIGH` (HR above 120 for 60 s, ward `*`), later tighten it to 110, add an ICU-B-only SpO2 rule, add a care plan for `P-1002` (SpO2 88–100), then delete `R-HR-HIGH`.

---

## A1: Parsing device frames ⭐

**Story.** The integration team wants every frame turned into a typed object before anything else happens.

**New classes:** `MapFunction`, `TypeInformation`, `Types`

### Requirements

1. Create a POJO `DeviceFrame`: `version`, `deviceId`, `patientId`, `wardId`, `timestamp` (long), `measurements` (`Map<String, String>` holding the raw codes and values), and `boolean valid` plus `String parseError`.
2. Implement `FrameParser implements MapFunction<String, DeviceFrame>`. A malformed frame must **not** throw. Return a frame with `valid = false` and a reason instead. (Why not throw? Think about what a single bad frame would do to the job.)
3. Print only the valid frames.
4. **Type exercise:** after parsing, write a lambda `map` that turns each valid frame into `Tuple2<String, Integer>` (patientId, number of measurements). Run it once without any type hint and read the error. Then fix it with `.returns(...)`, using `Types`.
5. **Type exercise:** print `TypeInformation.of(DeviceFrame.class)` and check that Flink sees a POJO (`PojoType<...>`) and not a `GenericType`. Then set `pipeline.generic-types: false` in the job's `Configuration` and confirm the job still runs.

### Acceptance criteria

- About 98% of frames parse as valid, and the rest carry a readable `parseError`.
- The job starts with generic types disabled.
- You can explain why the lambda needed `.returns(...)` but the `FrameParser` class did not.

<details><summary>Hint 1</summary>

Java erases generic type parameters from lambdas at compile time. Flink can read the type arguments from a class that implements `MapFunction<String, DeviceFrame>`, because they are recorded in the class signature. A lambda has no such signature.
</details>

<details><summary>Hint 2</summary>

`Types.TUPLE(Types.STRING, Types.INT)` builds a `TypeInformation<Tuple2<String,Integer>>`. Other factories to know: `Types.POJO(Class)`, `Types.MAP(k, v)`, `Types.LIST(e)`, `Types.ENUM(Class)`.
</details>

<details><summary>Hint 3</summary>

For a POJO to be recognized, the class must be public, have a public no-arg constructor, and every field must be public or have a getter and setter. A `Map<String,String>` field is fine. Flink serializes it with `MapTypeInfo`. You can check this with `((PojoTypeInfo<?>) info).getPojoFieldAt(i)`.
</details>

<details><summary>Hint 4</summary>

Generic types are switched off through `PipelineOptions.GENERIC_TYPES` on the `Configuration` you pass to `StreamExecutionEnvironment.getExecutionEnvironment(conf)`. Any class that falls back to Kryo will then fail at job-graph build time, not at runtime.
</details>

**Stretch:** Make `valid` and `parseError` unnecessary by returning `null` for bad frames. What happens? Why is that a bad idea, and which function type from A2 fixes it properly?

---

## A2: One frame, many readings ⭐

**Story.** Every downstream consumer wants one record per measurement, in normalized units.

**New classes:** `FlatMapFunction`, `Collector` (and more `Types`)

### Requirements

1. Create a POJO `VitalReading`: `patientId`, `wardId`, `deviceId`, `vitalCode` (String or an enum), `double value`, `long timestamp`.
2. Implement `FrameExploder implements FlatMapFunction<DeviceFrame, VitalReading>`:
   - Invalid frames produce **zero** outputs.
   - A valid frame produces **one output per measurement**.
   - Normalize units: `TEMP` always in °C. Encode `O2` as 1.0/0.0 and `CONSC` as `A`=0, `C`=1, `V`=2, `P`=3, `U`=4.
   - A single bad measurement (`HR=abc`) drops only that measurement, not the whole frame.
3. Rewrite the same logic as a lambda, `flatMap((frame, out) -> …)`, and make it compile and run. You will need `.returns(...)` again, this time with a POJO type.
4. Count the readings per `vitalCode` with a quick `keyBy(...).sum(...)` or similar, just to check that the explosion looks right.

### Acceptance criteria

- A frame with 8 measurements, one of them garbled, yields exactly 7 readings.
- 98.6F comes out as 37.0 (to one decimal place).

<details><summary>Hint 1</summary>

`Collector<T>.collect(t)` can be called 0, 1, or N times per input. That is the whole difference from `MapFunction`.
</details>

<details><summary>Hint 2</summary>

A lambda `flatMap` has *two* erased generics: the output type and the `Collector<T>` parameter. Flink's error message will say so. `Types.POJO(VitalReading.class)` or `TypeInformation.of(VitalReading.class)` both work.
</details>

<details><summary>Hint 3</summary>

Don't reuse one mutable `VitalReading` instance across `collect()` calls. With object reuse enabled, or with chained operators, the downstream operator may still hold a reference to it. Create a new object per reading.
</details>

**Stretch:** In a unit test, use a hand-rolled `Collector` that appends to a `List` to test `FrameExploder` without starting Flink at all.

---

## A3: The data-quality gate ⭐⭐

**Story.** Clinical engineering doesn't want bad data silently dropped. They want it routed to separate streams so they can find faulty devices.

**New classes:** `ProcessFunction`, `OutputTag`, `OpenContext`

### Requirements

1. Implement `QualityGate extends ProcessFunction<VitalReading, VitalReading>` (non-keyed):
   - **Main output:** clean readings.
   - **Side output `impossible`:** values outside physiological limits (HR not in 20–300, SpO2 not in 50–100, TEMP not in 25–45 °C, and so on).
   - **Side output `artifact`:** lead-off signatures (HR = 0, SpO2 = 0). You can only spot these per reading, so decide on a per-reading rule.
   - **Side output `clock-skew`:** readings whose timestamp is more than 5 minutes ahead of the operator's current processing time. The side-output type should be a *different* class, e.g. `SkewedReading(reading, skewMillis)`.
2. In `open(OpenContext)`, load the physiological-limits table into a field, and record the subtask index for logging.
3. Print each side output with its own prefix.
4. Try registering a timer inside `processElement`. Observe what happens and write one sentence explaining why.
5. Write a unit test with `ProcessFunctionTestHarnesses.forProcessFunction(...)` that checks what lands in each output (`harness.getSideOutput(tag)`).

### Acceptance criteria

- Each side output receives only its own kind of record.
- `SkewedReading` flows out of a side output typed differently from the main stream.

<details><summary>Hint 1</summary>

`OutputTag` has to keep its type after erasure. Either create it as an anonymous subclass (`new OutputTag<VitalReading>("impossible") {}`, note the `{}`) or pass a `TypeInformation` explicitly: `new OutputTag<>("clock-skew", Types.POJO(SkewedReading.class))`.
</details>

<details><summary>Hint 2</summary>

Emit with `ctx.output(tag, value)`. Read with `SingleOutputStreamOperator#getSideOutput(tag)`, called on the stream returned by `.process(...)`. The same tag instance (or an equal one) must be used in both places, so make tags `static final` constants.
</details>

<details><summary>Hint 3</summary>

`ProcessFunction` is a rich function, so it has `open(OpenContext)` and `getRuntimeContext()`. In Flink 2.x the subtask index comes from `getRuntimeContext().getTaskInfo()`. `ctx.timerService().currentProcessingTime()` gives you "now".
</details>

<details><summary>Hint 4</summary>

Timers are scoped to a key. A non-keyed stream has no key to attach the timer to, so Flink throws `UnsupportedOperationException`. That is your motivation for A4.
</details>

---

## A4: Silence and sustained tachycardia ⭐⭐

**Story.** Nurses need two alerts: *"a monitor has stopped sending data"* (it may have been unplugged) and *"heart rate has stayed above 120 for 60 seconds"*, since a single spike is usually noise.

**New classes:** `KeyedProcessFunction`, `ValueState`, `ValueStateDescriptor` (plus `OpenContext` again)

### Requirements

1. Assign event-time timestamps and watermarks to the clean stream (bounded out-of-orderness of 5 s).
2. `DeviceSilenceDetector extends KeyedProcessFunction<String, VitalReading, Alert>`, keyed by `patientId`:
   - After each reading, the patient must produce another reading within **30 s** of **processing time**, or you emit `Alert(DEVICE_SILENT)`.
   - Emit only one silence alert per silence. When readings resume, emit `Alert(DEVICE_RESUMED)`.
   - Keep **at most one** pending timer per patient.
3. `SustainedTachycardiaDetector extends KeyedProcessFunction<String, VitalReading, Alert>`, keyed by `patientId`, looking only at `HR`:
   - HR > 120 continuously for ≥ 60 s of **event time**: emit `Alert(TACHYCARDIA_SUSTAINED)` once.
   - Any HR ≤ 120 resets the episode.
   - Implement it **with an event-time timer**, not by comparing timestamps on the next reading. What happens with the comparison approach if the patient's HR stays high but the device goes silent?
4. Create all state in `open(OpenContext)` using `ValueStateDescriptor`s with explicit `Types`.
5. Test both functions with `ProcessFunctionTestHarnesses.forKeyedProcessFunction(...)`, driving time with `harness.setProcessingTime(...)` and `harness.processWatermark(...)`.

### Acceptance criteria

- `P-1013` produces exactly one `DEVICE_SILENT` and one `DEVICE_RESUMED` per scripted dropout.
- `P-1007` produces exactly one `TACHYCARDIA_SUSTAINED` alert per episode.
- The harness test shows that an HR reading of 115 at t = 59 s prevents the alert.

<details><summary>Hint 1</summary>

For the silence detector, store the timestamp of the currently registered timer in a `ValueState<Long>`. On each new reading, delete that timer (`deleteProcessingTimeTimer`) and register a new one. Without the delete, you pile up one timer per reading.
</details>

<details><summary>Hint 2</summary>

A second `ValueState<Boolean>` (or an enum state) can record "currently silent" so that you emit `DEVICE_RESUMED` exactly once.
</details>

<details><summary>Hint 3</summary>

For tachycardia, the state is "episode started at T" (`ValueState<Long>`). On the first high reading, set it and register an event-time timer at `T + 60s`. On a normal reading, clear the state **and** delete the timer. In `onTimer`, check that the state is still set before alerting.
</details>

<details><summary>Hint 4</summary>

`new ValueStateDescriptor<>("episodeStart", Types.LONG)`, then `getRuntimeContext().getState(descriptor)` inside `open`. Keyed state is automatically scoped to the current key, both in `processElement` and in `onTimer`.
</details>

<details><summary>Hint 5</summary>

`onTimer` can't tell which timer fired unless you encode it. Two detectors in two functions keeps things simple. If you merge them later, compare `ctx.timeDomain()` or keep the expected timestamps in state.
</details>

**Stretch:** Add `StateTtlConfig` so that patients who have been discharged (no data for 24 h) don't keep state forever.

---

## A5: NEWS2 early-warning score ⭐⭐⭐

**Story.** The hospital uses the **National Early Warning Score 2 (NEWS2)**. It combines seven vitals into one number that tells staff how urgently to respond. You need the latest value of *each* vital per patient, so one `ValueState` is no longer enough.

**New classes:** `MapState`, `MapStateDescriptor`

### NEWS2 scoring table (SpO2 scale 1)

| Parameter | 3 | 2 | 1 | 0 | 1 | 2 | 3 |
|---|---|---|---|---|---|---|---|
| RR (/min) | ≤8 | | 9–11 | 12–20 | | 21–24 | ≥25 |
| SpO2 (%) | ≤91 | 92–93 | 94–95 | ≥96 | | | |
| Supplemental O2 | | Yes | | No | | | |
| SBP (mmHg) | ≤90 | 91–100 | 101–110 | 111–219 | | | ≥220 |
| HR (/min) | ≤40 | | 41–50 | 51–90 | 91–110 | 111–130 | ≥131 |
| Consciousness | | | | Alert | | | C/V/P/U |
| Temp (°C) | ≤35.0 | | 35.1–36.0 | 36.1–38.0 | 38.1–39.0 | ≥39.1 | |

Risk bands: total 0–4 is **LOW**. Any single parameter scoring 3 is **LOW-MEDIUM** (urgent ward review). Total 5–6 is **MEDIUM**. Total ≥7 is **HIGH**.

### Requirements

1. `News2Scorer extends KeyedProcessFunction<String, VitalReading, News2Score>`, keyed by `patientId`.
2. Keep `MapState<String, VitalReading>`: the **latest** reading per `vitalCode` (ignore readings older than the one already stored).
3. After each update, compute the score **only if all seven parameters have a reading no older than 5 minutes** (event time). Otherwise emit nothing, and send a `ScoreUnavailable(patientId, missingCodes)` to a side output at most once per minute.
4. Emit a `News2Score(patientId, total, band, perParameter)` **only when the total or the band changes**. Use a `ValueState` for the last emitted score.
5. Register a cleanup timer that removes entries older than 30 minutes from the `MapState`.
6. Use explicit types: `new MapStateDescriptor<>("latestVitals", Types.STRING, Types.POJO(VitalReading.class))`.

### Acceptance criteria

- `P-1007`'s score rises through the bands as the scripted deterioration unfolds.
- Stable patients produce a score once and then stay quiet.
- A harness test shows that a missing `SBP` blocks scoring and a fresh `SBP` unblocks it.

<details><summary>Hint 1</summary>

`MapState` behaves like a per-key `Map`: `get`, `put`, `contains`, `remove`, `entries()`, `keys()`, `values()`, `isEmpty()`. Each entry is serialized separately, which is why it beats a `ValueState<Map<…>>`: with RocksDB, updating one vital doesn't rewrite the whole map.
</details>

<details><summary>Hint 2</summary>

Keep the scoring table as a pure static function, `int score(String code, double value)`, and unit-test it on its own against the table above. Most NEWS2 bugs are boundary bugs (91 vs 92, 35.0 vs 35.1).
</details>

<details><summary>Hint 3</summary>

When cleaning up during iteration, collect the keys to remove first and remove them after the loop. Don't modify the map while iterating over `entries()`.
</details>

<details><summary>Hint 4</summary>

For "at most once per minute" on the side output, keep a `ValueState<Long> lastUnavailableEmitted`. No timer is needed.
</details>

**Stretch:** Store a `RunningStats` POJO per vital in a second `MapState` (count, mean, M2 for Welford's variance) and flag readings more than 3σ from this patient's own baseline.

---

## A6: Clinician-configurable thresholds ⭐⭐⭐

**Story.** Hard-coded thresholds won't do. Clinicians want to change alert rules while the job runs, with no redeploy. Each rule must apply to every reading, on every parallel instance.

**New classes:** `BroadcastProcessFunction`, `ReadOnlyBroadcastState` (and `MapStateDescriptor` in a new role)

### Requirements

1. Parse the rules feed (from 0.3) into `RuleCommand` objects with a `FlatMapFunction`, so malformed commands produce 0 outputs. Ignore the `CAREPLAN` lines for now.
2. Broadcast the rules with a `MapStateDescriptor<String, ThresholdRule>` keyed by `ruleId`.
3. `ThresholdAlerter extends BroadcastProcessFunction<VitalReading, RuleCommand, Alert>`:
   - `processBroadcastElement`: apply `UPSERT` and `DELETE` to the broadcast state. Also emit a `RuleAudit` record to a side output for each change.
   - `processElement`: through the **`ReadOnlyBroadcastState`**, find every rule that matches the reading's `vitalCode` and ward (`*` matches all wards), and emit `Alert(THRESHOLD_BREACH, ruleId, value)` when the value falls outside [min, max]. Ignore `sustainSeconds` for now.
   - Ward-specific rules **override** `*` rules for the same vital.
4. Before any rules have arrived, fall back to a built-in default rule set, so you don't raise zero alerts.
5. Test it with `ProcessFunctionTestHarnesses.forBroadcastProcessFunction(...)`: send a rule, a breaching reading, a `DELETE`, then the same reading again.

### Acceptance criteria

- Tightening `R-HR-HIGH` from 120 to 110 during the run changes alerts from that moment on, with no restart.
- The `RuleAudit` side output shows every rule change **once per parallel instance**. Explain why you see it more than once.

<details><summary>Hint 1</summary>

The same `MapStateDescriptor` instance must be passed to `rulesStream.broadcast(descriptor)` **and** used inside the function (`ctx.getBroadcastState(descriptor)`). Make it a `static final` constant.
</details>

<details><summary>Hint 2</summary>

In `processElement` you only get a `ReadOnlyBroadcastState`. Iterate with `immutableEntries()`. Don't mutate the rule objects you get back, even though Java would let you. Flink's guarantee that every instance holds identical state depends on the state changing only in `processBroadcastElement`.
</details>

<details><summary>Hint 3</summary>

The logic in `processBroadcastElement` must be **deterministic**: no `System.currentTimeMillis()`, no random numbers, no external lookups. Every parallel instance runs it separately and must end up with the same state.
</details>

<details><summary>Hint 4</summary>

There is no ordering guarantee between the two inputs. A reading can arrive before a rule that was "sent earlier". That is why requirement 4 exists. Why can't you buffer early readings in keyed state here?
</details>

---

## A7: Sustained breaches and per-patient care plans ⭐⭐⭐⭐

**Story.** Clinicians want the rules from A6 to honour `sustainSeconds` (e.g. *"SpO2 below 90 for 120 s"*). They also want **care-plan overrides** for individual patients: `P-1002` has COPD, and alerts for an SpO2 of 88–92 would cause alarm fatigue. This needs broadcast rules *and* per-patient state and timers.

**New class:** `KeyedBroadcastProcessFunction`

### Requirements

1. Broadcast two kinds of state through one broadcast stream: rules (`MapStateDescriptor<String, ThresholdRule>`) and care plans (`MapStateDescriptor<String, CarePlan>`, keyed by `patientId + ":" + vitalCode`).
2. `SustainedBreachDetector extends KeyedBroadcastProcessFunction<String, VitalReading, RuleCommand, Alert>`, with the readings keyed by `patientId`:
   - Keyed `MapState<String, Long> breachStart`, from `ruleId` to the event time when the breach began.
   - On a breaching reading with no episode open: record the start and register an event-time timer at `start + sustainSeconds`.
   - On a non-breaching reading: clear that rule's episode.
   - In `onTimer`: **re-check through the read-only broadcast state** that the rule still exists and the episode is still open, then alert. The rule may have been deleted or changed while the timer was pending.
   - A care plan for the patient and vital **replaces** the rule's min/max for that patient.
   - Each alert must say whether a care plan affected it.
3. When a rule is **deleted**, remove its episodes for **all** patients from within `processBroadcastElement`.
4. Send any alert that is suppressed only because of a care plan to a `suppressed-by-careplan` side output, for audit.
5. Test it with `ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(...)`, and include a "rule deleted while the timer is pending" case.

### Acceptance criteria

- `P-1002` no longer fires SpO2 alerts once the care plan arrives, and these appear in the audit side output instead.
- Deleting a rule while an episode is open results in no alert when its timer fires, and leaves no stale `breachStart` entries behind.

<details><summary>Hint 1</summary>

`processBroadcastElement` has **no current key**. You can't read or write keyed state for "this patient" there, and you can't register timers. What you *can* do is `ctx.applyToKeyedState(stateDescriptor, (key, state) -> …)`, which visits every key's state on this instance.
</details>

<details><summary>Hint 2</summary>

To use `applyToKeyedState`, you need the `MapStateDescriptor<String, Long>` of `breachStart` as a field or constant, not just the `MapState` handle.
</details>

<details><summary>Hint 3</summary>

In `processElement` and `onTimer`, `ctx.getBroadcastState(descriptor)` returns a `ReadOnlyBroadcastState`. The timer callback can see the *current* rules, not the rules as they were when the timer was registered. Use that to your advantage.
</details>

<details><summary>Hint 4</summary>

Several rules can have timers for the same patient. A timer only tells you its timestamp. Either iterate over `breachStart` in `onTimer` and fire every episode whose `start + sustain <= timestamp`, or keep a separate `MapState<Long, List<String>>` from timer timestamp to ruleIds.
</details>

<details><summary>Hint 5</summary>

Timers are de-duplicated per (key, timestamp). If two rules share the same deadline you get **one** callback. The iteration approach in Hint 4 handles this for free.
</details>

---

## A8: Reliable paging with operator state ⭐⭐⭐⭐

**Story.** Alerts go to the nurses' pager gateway, which charges per call and asks callers to **batch** up to 10 alerts per page. It also needs **suppression**: the same (patient, alertType) must not be paged twice within 2 minutes. The alert stream at this point is *not keyed*, and paging must not lose an alert when the job fails over.

**New classes:** `CheckpointedFunction`, `FunctionInitializationContext`, `FunctionSnapshotContext` (with `FlatMapFunction` and `Collector` again)

### Requirements

1. Union all alert streams (A4, A6, A7, plus NEWS2 band changes mapped to `Alert` with a `MapFunction`).
2. `PagerBatcher implements FlatMapFunction<Alert, PageBatch>, CheckpointedFunction`, running with parallelism 2:
   - Buffer alerts in memory (a `List<Alert>`). When the buffer reaches 10, emit one `PageBatch` and clear it.
   - Also flush a non-empty buffer if the incoming alert is `HIGH` severity. Urgent pages can't wait.
   - Suppression: keep a `Map<String, Long>` from `patientId|alertType` to the last paged event time. Drop repeats within 2 minutes, and evict old entries.
3. `initializeState(FunctionInitializationContext)`:
   - Obtain **operator** list state for the buffer and the suppression entries through `context.getOperatorStateStore()`.
   - If `context.isRestored()`, rebuild the in-memory buffer and map from it and log how many alerts were recovered.
4. `snapshotState(FunctionSnapshotContext)`: copy the in-memory buffer and map into the list state, and log `context.getCheckpointId()`.
5. **Failure drill:** enable checkpointing (every 5 s) and a fixed-delay restart strategy. Inject one failure: throw from `flatMap` on the 200th alert, **only on the first attempt**. Show that no buffered alert is lost. Each alert id should appear in the output at least once.
6. **Rescale drill (harness):** take a snapshot of a parallelism-2 operator with `OneInputStreamOperatorTestHarness` (wrapping `new StreamFlatMap<>(new PagerBatcher())`) and restore it into parallelism 1 and parallelism 3. Try both `getListState` and `getUnionListState` for the *suppression* map and explain which one is correct for it, and which one is correct for the buffer.

### Acceptance criteria

- After the injected failure, every alert id that entered `PagerBatcher` before the crash appears in some `PageBatch`.
- You can explain why you see duplicates (at-least-once) and what a real sink would need for exactly-once.
- You can explain even-split versus union redistribution when rescaling, with your harness results.

<details><summary>Hint 1</summary>

The pattern is: work on plain Java fields during processing, and copy them into `ListState` only in `snapshotState` (`state.update(list)`). In `initializeState`, create the state handles *first*, then read them back only if `isRestored()`.
</details>

<details><summary>Hint 2</summary>

`new ListStateDescriptor<>("buffer", Types.POJO(Alert.class))`. For the suppression map, store entries as `Tuple2<String, Long>` with `Types.TUPLE(Types.STRING, Types.LONG)`. (`ListState` and `ListStateDescriptor` aren't on the list, but operator state requires them.)
</details>

<details><summary>Hint 3</summary>

`getListState` (even-split) hands each new subtask a *slice* of all entries. `getUnionListState` hands *every* subtask *all* entries. A buffered alert must be paged exactly once, so should it be duplicated to every subtask? Suppression entries are only useful where the same (patient, type) arrives, and after rescaling you don't know where that is.
</details>

<details><summary>Hint 4</summary>

For "throw only on the first attempt", `getRuntimeContext().getTaskInfo().getAttemptNumber()` is available if you also extend `AbstractRichFunction` (or use `RichFlatMapFunction`). A static flag is fragile across JVMs, but it works in a local `MiniCluster`.
</details>

<details><summary>Hint 5</summary>

Harness snapshot and restore: `OperatorSubtaskState s = harness.snapshot(checkpointId, ts);`, then build a new harness, call `initializeState(s)` (or repartition with `AbstractStreamOperatorTestHarness.repartitionOperatorState(...)` for rescaling), and then `open()`.
</details>

**Stretch:** `FunctionInitializationContext#getKeyedStateStore()` returns non-null only on keyed streams. Make a keyed variant and discuss when you would prefer it.

---

## A9: Capstone, VitalWatch in production ⭐⭐⭐⭐⭐

**Story.** Put it all together as one job that the on-call team can operate.

### Requirements

1. One launcher, `VitalWatchJob`, that wires A1–A8 into a single topology, following the diagram at the top.
2. Stable `.uid(...)` and `.name(...)` on **every** stateful operator (A4, A5, A6, A7, A8).
3. `pipeline.generic-types: false` for the whole job, fixing any class that falls back to Kryo.
4. **Savepoint drill** (with the local cluster from `docker-compose.yml`):
   - Run for 5 minutes, then stop with a savepoint.
   - Change the NEWS2 "change-only" logic (a code change) and restore. State must survive.
   - Rename a state descriptor in A5 and restore. Observe what happens and explain it.
   - Add a new field to `VitalReading` and restore. Does POJO schema evolution work here?
5. **Operational report** (a short markdown file) answering:
   - Which operators hold keyed state, broadcast state, or operator state, and roughly how big each grows per patient and per rule.
   - Where event time is used and where processing time is used, and why each choice is right for its alert.
   - What happens to each alert type if the rules feed stalls, if one device's clock drifts, or if all devices go silent (watermarks stop advancing).

### Acceptance criteria

- The job survives a savepoint restore without losing open tachycardia episodes, NEWS2 latest vitals, rules, care plans, or buffered pages.
- Every class in the coverage map appears somewhere in `VitalWatchJob`'s code path.

<details><summary>Hint 1</summary>

Broadcast state is snapshotted by *every* instance. On restore with a different parallelism, each new instance gets a copy. That only works because every instance's copy was identical, which is why the determinism rule from A6 matters.
</details>

<details><summary>Hint 2</summary>

State is matched on restore by *operator uid + state descriptor name*. Changing either one orphans that state. `--allowNonRestoredState` lets the job start anyway, but it doesn't bring the state back.
</details>

<details><summary>Hint 3</summary>

If every source partition goes quiet, the watermark stops advancing and event-time timers (A4 tachycardia, A7 sustained breaches) stop firing. Look at `WatermarkStrategy#withIdleness` and ask whether it would actually help when *all* inputs are idle. The processing-time silence detector in A4 is exactly what still works in that case.
</details>

---

## Suggested pace

| Assignment | Difficulty | Rough time |
|---|---|---|
| A0 setup | — | 1–2 h |
| A1 Map + Types | ⭐ | 1 h |
| A2 FlatMap + Collector | ⭐ | 1 h |
| A3 ProcessFunction + OutputTag + OpenContext | ⭐⭐ | 2 h |
| A4 KeyedProcessFunction + ValueState | ⭐⭐ | 3 h |
| A5 MapState + NEWS2 | ⭐⭐⭐ | 3–4 h |
| A6 BroadcastProcessFunction | ⭐⭐⭐ | 3 h |
| A7 KeyedBroadcastProcessFunction | ⭐⭐⭐⭐ | 4–5 h |
| A8 CheckpointedFunction | ⭐⭐⭐⭐ | 4–5 h |
| A9 Capstone | ⭐⭐⭐⭐⭐ | 1 day |

When you're stuck after the hints, or want to compare your code with a reference, ask for **"the solution for A*n*"** (or just one requirement, e.g. "A7 requirement 3").
