# Implementation Plan: Simulating Patient Discharges & Bed Occupancy

## Motivation

A real hospital has admissions **and** discharges. Regions should discharge at
different, sometimes-slower-than-admission rates so that a hospital's bed
occupancy can realistically climb and, in the worst case, run out of beds.
This document records the design that was discussed and agreed upon before
any code was written, so the rationale is preserved.

## Revision log

**Rev 2 (2026-09-27) — Flink streaming best-practices review.** The original
decisions 1–4 still stand. The implementation details changed where they would
have caused bugs, or where they went against idiomatic Flink:

| # | Original plan | Problem | Revised plan |
|---|---|---|---|
| R1 | Simulator keyed by `hospitalID`, `MapState<Long timerTime, String patientID>` | Flink **deduplicates timers per (key, timestamp)**, and the map holds only one ID per timestamp. Two patients at the same hospital with the same discharge millisecond would share one timer and one map entry, so one discharge would be **silently lost** and that bed would stay occupied forever. There are also only 14 keys, so work is skewed | Key the simulator by **`patientID`**: one key → one pending discharge → one timer, held in `ValueState`. Collisions become impossible, and keys spread evenly across subtasks (see decision 5) |
| R2 | Processing-time timer at `admitTime + los` | Mixes clocks (`admitTime` is event time). Records emitted from `onProcessingTime` have **their timestamp erased** (confirmed in `KeyedProcessOperator` bytecode: `eraseTimestamp()`), so downstream event-time logic breaks. Tests depend on wall-clock | **Event-time timers** at `ctx.timestamp() + los`. `onEventTime` stamps output with the timer time (`setAbsoluteTimestamp`). Tests drive time with watermarks (see decision 6) |
| R3 | Random LOS drawn in `processElement` | Not deterministic. On failure/replay a patient gets a different LOS, and unit tests can't assert exact discharge times | LOS is a **pure function of `patientID` + region profile** (seeded from the ID), and is injected so tests can pass a fixed LOS (see decision 7) |
| R4 | `CapacityAlert` extended with a `status` field | `CapacityAlert` uses Lombok `@AllArgsConstructor`, so adding a field changes the constructor that `HospitalCapacityMonitor` calls. That **breaks Lesson2A**, which contradicts decision 1 | New **`BedOccupancyAlert`** model. `CapacityAlert` stays untouched |
| R5 | Alert on every threshold transition | Occupancy that hovers around 90% would **flap** (alert storm) | Keep "re-fire on every transition", but with **hysteresis** bands (see decision 4) |
| R6 | "Overflow whenever admission rate exceeds `1 / meanLengthOfStay`" | Wrong model. Beds are not modelled as servers that queue patients: occupancy is just a count, so this is an M/G/∞ system | Use **Little's law**, `L = λ · W`: a hospital overflows when `λ_hospital · meanLOS > capacity`. Worked numbers below |
| R7 | No checkpointing, no operator UIDs, no metrics | Keyed state and timers are only fault-tolerant with checkpoints. Savepoints need stable UIDs. The occupancy floor-at-0 would hide bugs | Enable checkpointing, set `uid()` / `name()` on every stateful operator, add counters (see "Operational best practices") |

## Key design questions & decisions

1. **Should the existing `Lesson2A` / `HospitalCapacityMonitor` (admit-only,
   monotonically-increasing occupancy) be modified?**
   **Decision: No.** Leave `Lesson2A` and `HospitalCapacityMonitor` completely
   untouched. The new admit+discharge bed-occupancy tracking is introduced as
   a **new** lesson (`Lesson2B`) and a **new** monitor class, so the existing
   lesson keeps teaching its original, simpler concept. *(Rev 2: this also
   rules out changing shared classes in ways that break binary compatibility.
   That applies to `CapacityAlert` (see R4) and to the watermark strategy in
   `PatientAdmissionSource`, which both lessons share.)*

2. **Should `DischargeEvent.patientID` be a synthetic/independent ID, or
   should it reference a real, previously-admitted patient?**
   **Decision: Real, previously-admitted patient ID.** A synthetic ID
   (generated independently by a second, unrelated source) was rejected as
   unrealistic.

3. **How should a real patient ID be threaded from admission to discharge
   without shared mutable state across JVMs (i.e., without breaking on a real
   distributed cluster)?**
   Two options were considered:
   - **Rejected:** Two independent sources (an admit source and a discharge
     source) coordinated through a shared **static, mutable, in-JVM
     registry** of "currently admitted patients per hospital". This only
     works because this project's `run.sh` happens to execute everything in
     one JVM (Flink's local MiniCluster). On a real distributed cluster, each
     TaskManager JVM would have its own disjoint copy of that static
     registry, silently breaking the "real patientID" linkage. It also has an
     awkward startup edge case (what does the discharge generator do before
     any admission has landed yet?), which would require a hacky
     sentinel/no-op discharge event because a Flink `GeneratorFunction` must
     always return a value. *(Note: **immutable** static reference data, like
     `HospitalCapacityRegistry`, is fine. The problem is **mutable** state
     shared through statics.)*
   - **Chosen: Timer-based, single-source design.** Keep a **single** admit
     source. A new `KeyedProcessFunction` stores each admitted patient's
     pending discharge in keyed state, derives a **region-parameterized
     length-of-stay**, and registers a Flink **event-time timer** for that
     future discharge moment. When the timer fires, the *same, real*
     `patientID` already held in state is emitted as a `DischargeEvent` via a
     **side output**. This is fully Flink-idiomatic and needs no shared static
     state. It works correctly on a real distributed cluster because Flink
     manages timers and keyed state per key, is checkpointed, and is
     redistributed on rescale by key group.

     "Some regions discharge slower" is modelled with a **region-parameterized
     length-of-stay distribution** (e.g., region `"S"` gets a much longer mean
     stay). This is more realistic than biasing a second generator's
     hospital-selection weights. *(Rev 2, corrected, see R6:)* Occupancy
     behaves like an M/G/∞ system, so by **Little's law** steady-state
     occupancy is `L = λ_hospital · meanLOS`. A hospital structurally
     overflows whenever `λ_hospital · meanLOS > capacity`, and alerts whenever
     it exceeds `0.9 · capacity`. It no longer depends on two independent
     random generators happening to drift out of balance.

4. **Should a `BedOccupancyAlert` re-fire every time bed occupancy crosses a
   threshold, or only once ever?**
   **Decision: Re-fire on every status transition** (not a one-time-ever
   latch), so operators see every crossing, in both directions.
   *(Rev 2, see R5:)* Transitions use **hysteresis** so noise around a
   boundary doesn't cause an alert storm:

   | Status | Enter when | Leave (downward) when |
   |---|---|---|
   | `NORMAL` | ratio < 0.85 | — |
   | `HIGH` | ratio ≥ 0.90 (`HospitalCapacityRegistry.ALERT_THRESHOLD_RATIO`) | ratio < 0.85 |
   | `FULL` | ratio ≥ 1.00 | ratio < 0.95 |

   An alert is emitted **only when the stored status changes**, and is not
   re-emitted on every event.

5. **(Rev 2, new) What key should the lifecycle simulator use?**
   **Decision: `patientID`.** Each key has at most one pending discharge, so
   state is a single `ValueState<DischargeEvent>` and there is exactly one
   timer per key. This makes the timer-dedup/collision bug in R1 impossible.
   It also spreads load evenly (unlike 14 hospital keys; see
   `docs/data_skew.md`) and makes cleanup trivial (`clear()` in `onTimer`).
   The occupancy monitor is still keyed by `hospitalID`, which is correct
   because occupancy is a per-hospital aggregate.
   *Uniqueness assumption:* `PatientAdmissionGeneratorFunction` derives IDs
   from `index % 900000`, which is unique for the first 900k admissions
   (≈5 days at 2/s). If an admit arrives for a key that already has a
   pending discharge, the simulator treats it as a **re-admission conflict**:
   it logs, increments a `duplicateAdmissions` counter and keeps the existing
   stay (the new admission is still forwarded, so occupancy stays correct).
   Plan for this case explicitly; don't let it overwrite state silently.

6. **(Rev 2, new) Processing time or event time for the discharge timer?**
   **Decision: event time.** `admitTime` is already the event timestamp, and
   `PatientAdmissionSource` already assigns
   `forMonotonousTimestamps()` watermarks, so no source change is needed.
   Benefits:
   - Discharge records carry a real timestamp (the timer time), so downstream
     event-time windows or joins can be added later. Processing-time timers
     erase output timestamps.
   - Tests are deterministic: advance time with `processWatermark(...)`
     instead of wall-clock.
   - Replay after failure produces the same discharge times (together with
     decision 7).

   Trade-offs to document in the lesson:
   - Event-time timers fire only when the **watermark advances**. The
     watermark is the minimum across parallel source subtasks. If admissions
     stop, pending discharges stop too. That is acceptable for a simulation.
     If a subtask could go idle, add `.withIdleness(...)` in a **Lesson2B-only**
     overload of the source factory rather than changing the shared strategy
     (decision 1).
   - Discharge latency is about LOS plus the auto-watermark interval (200 ms by
     default).

7. **(Rev 2, new) How is length-of-stay drawn?**
   **Decision: deterministic and injectable.** A `Serializable`
   `LengthOfStaySampler` computes
   `los = profile(region).sample(new SplittableRandom(seedFrom(patientID)))`.
   Use exponential or log-normal with a region mean, **clamped** to
   `[minLos, maxLos]` (e.g. max = 5 × mean) so the timer horizon is bounded.
   It's pure (same patient → same LOS), so replays are stable. Tests pass a
   `FixedLengthOfStaySampler` to assert exact discharge timestamps. Any
   non-serializable helpers are created in `open()` and marked `transient`.

## Sizing the simulation (Little's law)

Per-hospital arrival rate: `λ_h = R × ¼ (region) × 1/n_hospitals(region)`,
with `R` = total admission rate. Alert when `λ_h · meanLOS ≥ 0.9 · capacity`.

At the default `R = 2/s`:

| Region | Hospitals | λ per hospital | Capacity | meanLOS to reach 90% | Suggested meanLOS | Steady-state L |
|---|---|---|---|---|---|---|
| NE | 4 | 0.125/s | 200 | 1440 s | 600 s | 75 (38%) |
| MW | 3 | 0.167/s | 170 | 918 s | 600 s | 100 (59%) |
| W | 5 | 0.100/s | 300 | 2700 s | 600 s | 60 (20%) |
| **S** | 2 | 0.250/s | 500 | 1800 s | **2400 s** | **600 (120% → FULL)** |

With exponential LOS, occupancy approaches steady state as
`L(t) = L∞ · (1 − e^(−t/meanLOS))`, so region S at these settings takes
≈55 min to hit `HIGH`. That is too slow for a demo. **Scale time:**
multiplying `R` by *k* and dividing every meanLOS by *k* keeps every
steady-state `L` identical but gets there *k*× faster. For example, with
`ADMISSION_RATE_PER_SECOND=20` and `LOS_SCALE=0.1`, S reaches `HIGH` in
≈5.5 min and `FULL` in ≈7 min. Expose both as environment overrides, the same
way `Lesson2A` uses `ADMISSION_WINDOW_SECONDS`.

## Confirmed Flink 2.3 APIs backing this design

Checked against the official
[Flink 2.3 Javadoc](https://nightlies.apache.org/flink/flink-docs-release-2.3/api/java/index.html).
Where the Javadoc was thin, checked with `javap` against the project's installed
`flink-runtime-2.3.0-tests.jar` / `flink-runtime-2.3.0.jar` (Rev 2 re-verified
the rows marked ✱):

| Need | Flink 2.3 API |
|---|---|
| Per-patient "wake up later and discharge" | `KeyedProcessFunction<K,I,O>`: `processElement(I, Context, Collector<O>)` + `onTimer(long, OnTimerContext, Collector<O>)` |
| Scheduling the future discharge ✱ | `ctx.timerService().registerEventTimeTimer(ctx.timestamp() + los)`. `KeyedProcessOperator.onEventTime` calls `setAbsoluteTimestamp(timer)`, whereas `onProcessingTime` calls `eraseTimestamp()` |
| Emitting a `DischargeEvent` without mixing types into the main output | `ctx.output(OutputTag<DischargeEvent>, value)` from inside `onTimer`. Declare the tag as `new OutputTag<DischargeEvent>("discharges") {}`. The anonymous subclass keeps the generic type, avoiding a Kryo fallback |
| Pulling the discharge stream back out downstream | `SingleOutputStreamOperator.getSideOutput(OutputTag<DischargeEvent>)` |
| Joining the two streams to track bed occupancy | `DataStream.connect(...)` → `ConnectedStreams.keyBy(sel1, sel2)` → `.process(KeyedCoProcessFunction<K,IN1,IN2,OUT>)` |
| No-mock unit test for the lifecycle simulator ✱ | `ProcessFunctionTestHarnesses.forKeyedProcessFunction(...)`, `harness.processElement(value, timestamp)`, `harness.processWatermark(long)`, `harness.getSideOutput(OutputTag)`, `harness.numEventTimeTimers()` |
| Fault-tolerance test (timers survive restore) ✱ | `AbstractStreamOperatorTestHarness.snapshot(long, long)` → `OperatorSubtaskState`, then `initializeState(OperatorSubtaskState)` on a fresh harness before `open()` |
| No-mock unit test for the bed-occupancy monitor ✱ | `ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(KeyedCoProcessFunction, KeySelector<IN1,K>, KeySelector<IN2,K>, TypeInformation<K>)`, `processElement1/2(value, ts)`, `processWatermark1/2` |

Also confirmed: `RateLimiterStrategy` (2.3 Javadoc) only supports
`perSecond(double)` / `perCheckpoint(long)` / `noOp()`. That is a single
uniform rate for the entire source, split evenly across parallel subtasks.
There is no per-key/per-region rate configuration, so "some regions discharge
slower" must be modelled with the region-parameterized length-of-stay
distribution described above, not with the rate limiter.

## Planned classes & changes

1. **`DischargeEvent`** (new, `data/model`): thin fact record with
   `patientID`, `hospitalID`, `admitTime`, `dischargeTime`. `patientID` is
   always a *real* ID copied from a prior `AdmitEvent`. `dischargeTime` is the
   **timer timestamp**, not `System.currentTimeMillis()`. Follow the existing
   model conventions (Lombok `@Getter @Setter @NoArgsConstructor
   @EqualsAndHashCode`, `Serializable`, no stored `regionID`; derive it through
   `HospitalCapacityRegistry` like `AdmitEvent` does) so Flink treats it as a
   **POJO** and not a Kryo generic type.

2. **`LengthOfStaySampler`** (new, `data/source/hospital`): a `Serializable`
   interface plus a `RegionLengthOfStaySampler` implementation. It holds an
   immutable `Map<String region, LengthOfStayProfile>`, and the LOS is a pure
   function of `(patientID, region)` (decision 7).

3. **`PatientLifecycleSimulator`** (new, `data/source/hospital`):
   `KeyedProcessFunction<String /* patientID */, AdmitEvent, AdmitEvent>`.
   - `public static final OutputTag<DischargeEvent> DISCHARGE_TAG`.
   - Constructor takes a `LengthOfStaySampler`.
   - State: `ValueState<DischargeEvent> pendingDischarge`, with a stable
     descriptor name.
   - `processElement`: if `pendingDischarge` is already set → re-admission
     conflict (decision 5): log, count, keep the existing stay. Otherwise
     compute `dischargeTs = ctx.timestamp() + los`, store the pending
     `DischargeEvent`, and call `registerEventTimeTimer(dischargeTs)`. Always
     forward the `AdmitEvent` unchanged on the main output.
   - `onTimer`: read the pending event, `ctx.output(DISCHARGE_TAG, event)`,
     then **`pendingDischarge.clear()`**. Timer-driven cleanup means no state
     TTL is needed. Don't add TTL as well: TTL doesn't delete timers, so the
     two would disagree.
   - Metrics (in `open()`): counters `dischargesEmitted`,
     `duplicateAdmissions`.

4. **`HospitalAdmissionAnalytics.attachPatientLifecycleSimulator(stream, sampler)`**:
   wires `stream.keyBy(e -> e.getPatient().getPatientID()).process(new
   PatientLifecycleSimulator(sampler)).uid("patient-lifecycle-simulator").name("patient-lifecycle-simulator")`.
   It returns the `SingleOutputStreamOperator<AdmitEvent>`. Callers get
   discharges with `.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG)`.
   The existing `attachCapacityMonitor` stays unchanged.

5. **`HospitalBedOccupancyMonitor`** (new):
   `KeyedCoProcessFunction<String /* hospitalID */, AdmitEvent, DischargeEvent, BedOccupancyAlert>`.
   - State: `ValueState<Integer> occupied`, `ValueState<BedOccupancyStatus> status`.
   - `processElement1` (admit) → `+1`; `processElement2` (discharge) → `−1`.
   - **Underflow is a bug signal, not a normal case.** Floor at 0, but also
     increment an `occupancyUnderflows` counter and log a warning. Two-input
     operators do **not** guarantee ordering *across* inputs. This plan
     connects the simulator's *forwarded* admit output, so an admit always
     leaves the same subtask before its discharge, and LOS ≫ network latency.
     A non-zero counter therefore means something is wrong upstream.
   - Evaluate the hysteresis table (decision 4) against
     `HospitalCapacityRegistry.capacityForHospital(...)`. Skip hospitals where
     it returns `-1`. Emit a `BedOccupancyAlert` only when the status changes.
     `alertTime = ctx.timestamp()` (event time), not wall-clock.

6. **`BedOccupancyAlert`** + **`BedOccupancyStatus`** enum (new, `data/model`):
   `hospitalID`, `regionID`, `previousStatus`, `status`, `occupied`,
   `maxCapacity`, `occupancyPercentage`, `alertTime`. Replaces the original
   "extend `CapacityAlert`" item (R4). **`CapacityAlert` is not modified.**

7. **`Lesson2B`** (new launcher). It leaves `Lesson2A` /
   `HospitalCapacityMonitor` untouched (decision 1) and wires:
   admit source → `attachPatientLifecycleSimulator` → `admits.connect(discharges)`
   → `.keyBy(AdmitEvent::getHospitalID, DischargeEvent::getHospitalID)` →
   `HospitalBedOccupancyMonitor` (`uid("bed-occupancy-monitor")`) → `print("BED-OCCUPANCY")`.
   - `env.enableCheckpointing(10_000)` (see "Operational best practices").
   - Environment overrides: `ADMISSION_RATE_PER_SECOND`, `LOS_SCALE`
     (see "Sizing the simulation").
   - Add a `2B` case to `run.sh` / `docker-run.sh`.

8. **`PatientLifecycleSimulatorTest`** (new, no mocks), using
   `forKeyedProcessFunction` with a `FixedLengthOfStaySampler`:
   - Admit at `t` → `numEventTimeTimers() == 1`. Watermark `t + los − 1` →
     no discharge. Watermark `t + los` → exactly one `DischargeEvent` with the
     same `patientID`, timestamp `t + los`, and `numEventTimeTimers() == 0`.
   - Main output forwards every admit unchanged.
   - Re-admission conflict does not register a second timer or overwrite the
     pending stay.
   - **Snapshot/restore:** admit → `snapshot(...)` → new harness
     `initializeState(snapshot)` → advance watermark → discharge is emitted.
     This proves that timers and state survive failover.

9. **`RegionLengthOfStaySamplerTest`** (new): same patient → same LOS;
   region S mean > other regions; every value is within the clamp bounds.

10. **`HospitalBedOccupancyMonitorTest`** (new, no mocks), using
    `forKeyedCoProcessFunction`: `NORMAL → HIGH → FULL → HIGH → NORMAL`
    transitions with hysteresis (no re-alert when oscillating between 0.85
    and 0.90), underflow clamp, unknown hospital ignored, alerts keyed
    per hospital.

11. **POJO guard test** (in either test class):
    `assertInstanceOf(PojoTypeInfo.class, TypeInformation.of(DischargeEvent.class))`
    (same for `BedOccupancyAlert`). This catches an accidental Kryo fallback,
    which is slower and breaks state schema evolution.

## Operational best practices applied (Rev 2)

- **Checkpointing:** keyed state and timers (including all pending
  discharges) are recovered only from a checkpoint. `Lesson2B` enables it.
  `print()` isn't transactional, so console output after a restore can repeat.
  Point this out in the lesson; state stays exactly-once.
- **Stable operator UIDs and state descriptor names:** set them on both
  stateful operators so savepoints can be restored after code changes. Never
  rename a state descriptor without a migration plan.
- **State size is bounded:** simulator state is about
  `λ · meanLOS` entries, i.e. the patients currently admitted, and each one is
  cleared when its timer fires. Monitor state holds 14 keys. The heap state
  backend is fine. If LOS or rate grows by orders of magnitude, switch to
  RocksDB/ForSt, which also keeps timers off-heap.
- **Serialization:** all records and state are Flink POJOs or enums. Keep
  functions' non-serializable fields `transient` and initialise them in
  `open(OpenContext)`.
- **Observability:** counters `dischargesEmitted`, `duplicateAdmissions`,
  `occupancyUnderflows`, visible in the Flink UI. Underflows and duplicates
  should stay at 0 in a healthy run.
- **Keep the synchronous state API.** Flink 2.x's async state (State V2 /
  ForSt) is deliberately out of scope for this lesson, to keep the timer/state
  semantics easy to follow.
- **Don't touch shared code paths used by Lesson2A:** make any
  Lesson2B-specific source tweak (e.g. watermark idleness) through a new
  overload, not by editing the existing factory methods.

## Status

Planning complete and agreed (Rev 1). Rev 2 (2026-09-27) applies the Flink
best-practices review above.

**Implemented (2026-09-27).** All 11 planned items are in place:
`DischargeEvent`, `BedOccupancyStatus`, `BedOccupancyAlert`,
`LengthOfStaySampler` / `RegionLengthOfStaySampler`, `PatientLifecycleSimulator`,
`HospitalBedOccupancyMonitor`,
`HospitalAdmissionAnalytics.attachPatientLifecycleSimulator`, `Lesson2B`, and the
tests `PatientLifecycleSimulatorTest`, `RegionLengthOfStaySamplerTest` and
`HospitalBedOccupancyMonitorTest` (the POJO guard lives in the simulator test).
`FixedLengthOfStaySampler` is test-only. `Lesson2A`, `HospitalCapacityMonitor`,
`CapacityAlert` and `PatientAdmissionSource` are unchanged. `run.sh` already
accepted `2B`; `docker-run.sh` now forwards `ADMISSION_RATE_PER_SECOND` and
`LOS_SCALE` to the Flink client.

Implementation notes:
- The LOS profile is exponential, clamped to `[0.1 × mean, 5 × mean]`. The seed
  is `patientID.hashCode()`, which the JLS specifies, so it's the same on every
  JVM. Unknown or `null` regions fall back to the 10-minute profile.
- Re-admission conflict (decision 5): the forwarded duplicate admit is counted
  by the monitor, but only one discharge follows, so each conflict leaks one
  bed. That only happens after patient IDs wrap (900k admissions). Watch the
  `duplicateAdmissions` counter.
- Smoke run: `ADMISSION_RATE_PER_SECOND=200 LOS_SCALE=0.01 ./run.sh 2B` took
  HS1/HS2 to `HIGH` and then `FULL` within about 40 s. NE, MW and W never
  alerted.
