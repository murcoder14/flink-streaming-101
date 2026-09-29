# Flink Exercises & Quizzes

Exercises built on this repo's code and data, using the Flink 2.3 APIs (`Duration` instead of `Time`, `open(OpenContext)`, Sink V2). No solutions are included.

**Suggested order:** Q1–Q5 → Ex 2 → Ex 1 → Ex 3 → Q6–Q8 → Ex 9 → Ex 4 → Ex 5 → Ex 6 → Ex 7 → Ex 8 → Ex 10 → Part C.

---

## Part A: Quick quiz (answer on paper, then check by running or reading the Javadoc)

**Q1. Parallelism and output.** In `Lesson1A`, parallelism is 2, but `windowAll(...)` runs with parallelism 1. How many lines does `print()` produce every 15 s? Which subtask prefixes (`1>`, `2>`) can appear, and why?

**Q2. Empty windows.** `RegionalAdmissionWindowFunction.process` (`HospitalAdmissionAnalytics.java:197`) falls back to `0L` when `elements` is empty. Can that branch ever run? What does this tell you about when Flink creates a window?

**Q3. Time domains.** `PatientAdmissionSource` sets up a `WatermarkStrategy` with an event-time assigner. `measureByRegion` then uses `TumblingProcessingTimeWindows`. Is the watermark used by that window at all? If the source replayed a day of old admissions in 10 seconds, what would the counts show? What would they show with `TumblingEventTimeWindows`?

**Q4. Aggregate versus buffer.** `measureByRegion` uses `aggregate(AggregateFunction, ProcessWindowFunction)`. `measureAcrossAllRegions` uses a plain `ProcessAllWindowFunction`. For a 2-minute window at 2 events/s, how many objects does each one keep in window state? When does `AggregateFunction.merge` actually get called? Hint: look at which window assigners are `MergingWindowAssigner`s.

**Q5. Skew.** `docs/data_skew.md` explains 100% skew for the NE-filtered `keyBy`. What if you keep the parallelism at 2 but key by `hospitalID` inside region S only (HS1 and HS2)? Could that still come out at 100%? Look up how `KeyGroupRangeAssignment` maps keys to subtasks.

**Q6. Timer de-duplication.** The Javadoc of `PatientLifecycleSimulator` says that keying by hospital would lose discharges. Write out a concrete two-event sequence that shows the loss, using `TimerService#registerEventTimeTimer` semantics.

**Q7. Ordering across inputs.** `HospitalBedOccupancyMonitor` says an underflow "means something is wrong upstream, because the discharge's admission is always forwarded before the discharge is emitted." After `connect(...).keyBy(...)`, the admits and the discharges travel on different network edges. Is the order of admit before discharge actually guaranteed at the co-process operator? Could `occupancyUnderflows` ever go above 0 without a bug? Argue it either way.

**Q8. Stalled event time.** In Lesson 2B, set `ADMISSION_RATE_PER_SECOND=0` after a while, or picture the source going quiet. What happens to pending discharges, and why? Which `WatermarkStrategy` method is meant to address idle inputs? Would it help in this case?

**Q9. Operator UIDs.** Lesson 2B sets `.uid(...)` on two operators but not on the source or the print sink. Suppose you take a savepoint, rename `"patient-lifecycle-simulator"` to something else, and restore. What happens? What changes if you instead change the state descriptor name `pendingDischarge`?

**Q10. POJO rules.** `AdmitEvent` has `getRegionID()` but no `regionID` field. Does Flink's POJO analyzer treat `regionID` as a serialized field? How can you check whether a class falls back to Kryo? Look at `PojoTypeInfo` and the `pipeline.generic-types` config.

---

## Part B: Hands-on exercises (write the code)

### Exercise 1: Event-time regional counts with late data *(windows, watermarks)*
Rewrite `measureByRegion` to use **event-time** tumbling windows. Then change `PatientAdmissionGeneratorFunction` so that about 10% of events get an `admitTime` 5 to 40 s in the past (out-of-order and late events).
- Use `WatermarkStrategy.forBoundedOutOfOrderness(...)`.
- Send events that arrive too late to a side output with `sideOutputLateData(OutputTag)` and print them separately.
- Try `allowedLateness(...)` and watch the same window **fire more than once**.
- **Question to answer:** how do the out-of-orderness bound and the allowed lateness each trade off latency against completeness?

### Exercise 2: Silent sensor detector *(KeyedProcessFunction, processing-time timers)*
`SensorSource` has sensor-3 stop reporting after about 20 s, and its comment says "useful for the timer lesson". That lesson hasn't been written yet. Write `Lesson3`:
- Key by `sensorId`. On every reading, (re)arm a timer 5 s ahead. Emit `"sensor-X went silent at …"` when the timer fires.
- You must **delete the previous timer**, because of timer de-duplication. Keep its timestamp in `ValueState`.
- Bonus: do it twice, once with processing-time timers and once with event-time timers. Explain why the event-time version might **never fire** for sensor-3. Connect that to Q8.

### Exercise 3: Sliding average temperature with an alert *(sliding windows, AggregateFunction)*
Compute a 30 s average per sensor that slides every 5 s. Drop faulty readings first. Use an `AggregateFunction<SensorReading, Acc, Double>` with a custom accumulator; don't buffer the readings.
- Emit an alert only when the average goes **above 30 °C after being below it**. That needs keyed state after the window.
- **Question:** how many windows does each reading belong to? What does that cost in state?

### Exercise 4: Length-of-stay statistics *(stream enrichment, keyed state)*
From the `DischargeEvent` stream in Lesson 2B, compute per region, **continuously**:
- the count of discharged patients, the mean length of stay, and the maximum length of stay.
- Do it once with `MapState` or an `AggregatingState` in a `KeyedProcessFunction`, and once with an event-time window. Compare the two outputs.
- Check the result against Little's law (`occupancy ≈ arrivalRate × meanLOS`), using the numbers in `docs/discharge_event_plan.md`.

### Exercise 5: Bus trip sessions *(session windows, custom watermarks)*
`BusGPSSource` emits with `noWatermarks()`. Change `Lesson1C` so that it:
- Assigns timestamps from `BusGPSEvent.timestamp`.
- Changes the reader so a bus sometimes goes quiet for 10 s or more (for example, parked).
- Uses `EventTimeSessionWindows.withGap(...)` keyed by `busId` to emit one "trip" per session: start, end, average speed and maximum passengers.
- **Question:** why can session windows *merge*, and what does this require from your aggregate (see Q4)?

### Exercise 6: Dynamic capacity rules *(broadcast state)*
Bed capacities are hard-coded in `HospitalCapacityRegistry`. Replace that lookup with a second stream of `CapacityUpdate(hospitalID, newCapacity)` events, generated slowly, for example once every 30 s.
- Use `admits.connect(updates.broadcast(descriptor))` with a `KeyedBroadcastProcessFunction`.
- A change in capacity should **re-evaluate the status** (NORMAL/HIGH/FULL) for that hospital, even when no admission arrives. Look at `applyToKeyedState` in `processBroadcastElement`.
- **Question:** why must `processBroadcastElement` be deterministic across all parallel instances?

### Exercise 7: Readmission detection *(interval join)*
Allow patient IDs to repeat. For example, 5% of admissions reuse a recently discharged patient's ID. Then detect **readmissions within 60 s of discharge**:
- `discharges.keyBy(patientID).intervalJoin(admits.keyBy(patientID)).between(Duration.ZERO, Duration.ofSeconds(60))`
- Emit `Readmission(patientID, dischargeTime, readmitTime, sameHospital?)`.
- Warning: repeating IDs breaks an assumption `PatientLifecycleSimulator` currently depends on (read its comment at line 70). Fix that assumption first. Then write a test for it with `ProcessFunctionTestHarnesses`.

### Exercise 8: State TTL for abandoned keys *(StateTtlConfig)*
In `HospitalCapacityMonitor` and `PatientLifecycleSimulator`, state is cleared only by explicit code. Add a `StateTtlConfig` to a state that could leak. Pick one and justify your choice.
- Explain why the comment in `PatientLifecycleSimulator.onTimer` says TTL "would not delete the timer".
- Explain which `UpdateType` and `StateVisibility` settings you chose, and why.

### Exercise 9: Test the co-process function *(test harnesses)*
Write `HospitalBedOccupancyMonitorOrderingTest` using `ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(...)`:
- Feed a discharge **before** its admission (see Q7) and assert the counter and output behavior.
- Walk through the hysteresis table, covering 0.84 → 0.90 → 1.00 → 0.96 → 0.94 → 0.84. Assert the exact alerts that are emitted.

### Exercise 10: Custom trigger, "early results" *(Trigger API)*
Keep the 2-minute regional window, but also emit a **speculative** partial count every 20 s, plus the final count at window end. Build it two ways:
1. With the built-in `ContinuousProcessingTimeTrigger` (or the event-time variant).
2. With your own `Trigger` subclass that fires early when the count reaches 50.
- **Question:** what is the difference between `FIRE` and `FIRE_AND_PURGE`, and how does each interact with an incremental `AggregateFunction`?

---

## Part C: Investigation challenges (Web UI and operations)

**C1. Chaining.** Run Lesson 2A in Docker and open the job graph. Add `.disableChaining()` or `.startNewChain()` in one place, and predict how the vertex count changes before you run it. Relate what you see to `docs/lesson2a.md`.

**C2. Failure recovery.** Run Lesson 2B with checkpointing, kill a TaskManager mid-run, and watch recovery. Check whether any pending discharge was lost, using the `dischargesEmitted` metric compared with the admissions count. Explain why the alerts from `print()` may be duplicated but occupancy is not.

**C3. Backpressure.** Run with `SENSOR_RATE=50000` and put an artificial `Thread.sleep(1)` in one map. Find the backpressured vertex in the UI and in the flame graph (`docs/FlameGraph.md`). Then explain the mailbox model's part in it (`docs/MailBox_Model.md`).

**C4. Async I/O.** Pretend `regionForHospital` is a slow remote lookup that takes about 200 ms. Replace it with `AsyncDataStream.unorderedWait(...)` and a `RichAsyncFunction`. Measure the throughput compared with a blocking `map`. What guarantees do you give up with `unorderedWait`, compared with `orderedWait`, with respect to watermarks?
