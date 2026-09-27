# Why "Records Sent" at the Source Doesn't Match the Sum You'd Expect

## Background

Job: "Hospital Patient Admissions 2-Minute Window Measurement" (see [Lesson2A.java](../src/main/java/org/muralis/flink/launcher/Lesson2A.java)).

The same `admissionsStream` (`DataStream<EmergencyRoom>`) is reused as input to two independent downstream chains in [HospitalAdmissionAnalytics.java](../src/main/java/org/muralis/flink/data/source/hospital/HospitalAdmissionAnalytics.java):

```java
DataStream<RegionalAdmissionCount> regionalCountsNE =
        HospitalAdmissionAnalytics.measureByRegion(admissionsStream, "NE", windowInterval);   // filters to region == "NE"

SingleOutputStreamOperator<EmergencyRoom> monitoredAdmissions =
        HospitalAdmissionAnalytics.attachCapacityMonitor(admissionsStream);                   // no filter — all 4 regions
```

Because the NE-filter chains directly with the Source (matching parallelism → `FORWARD` ship strategy), the JobGraph merges them into a single vertex: `Source: patient-admissions-source -> Filter`. That vertex has two outgoing edges — one to the NE-only window branch, one to the unfiltered capacity-monitor branch.

## Question 1

**Why is there a huge discrepancy between records sent by the first operator and the records received by the capacity-alert operator?**

## Question 2

**Are you sure the capacity branch is really seeing the unfiltered data? Isn't this Java's pass-by-reference at play, since the same `DataStream` object is passed to both branches?**

## Question 3

**I have a hard time believing that the creators of Flink would create a graph with confusing numbers.**

## Response summary

1. **No data is lost or duplicated in the actual pipeline.** Verified empirically by reproducing the real production topology (`measureByRegion` + `attachCapacityMonitor`) with a known, fixed input of 20 NE + 20 non-NE `EmergencyRoom` records — the capacity branch received all 40, including all 20 NE ones.
2. **It isn't Java pass-by-reference.** `DataStream` is a handle into Flink's logical stream graph, not a shared mutable data container. Reusing it as input to two chains adds two separate downstream vertices/edges from the same source node. At runtime, each admission record is independently serialized and sent down **each** outgoing edge — a genuine network-level replication, not object aliasing. This was proven with an isolated diagnostic (`Diag4`) using the real production classes.
3. **The "confusing numbers" are a real, documented, currently-open gap in Flink itself** — not a misunderstanding, and not something invented for this explanation.

   Apache Flink's own design wiki has **[FLIP-587: "Expose per downstream target `numRecordsOut` metric"](https://cwiki.apache.org/confluence/display/FLINK/FLIP-587%3A+Expose+per+downstream+target+numRecordsOut+metric)** (draft PR: [apache/flink#28014](https://github.com/apache/flink/pull/28014)), which names this exact scenario as one of three problem topologies:

   > *"**Multi-sink fan-out** - the same source/operator feeds several independent sinks in parallel."*

   And states the root cause plainly:

   > *"Flink tasks today report a single scalar `numRecordsOut` counter that counts every record emitted by the task's last operator, irrespective of how many distinct downstream consumers that output goes to. When a task has more than one network output, there is no way for any consumer (REST API, metric reporter, Web UI, Flink Kubernetes Operator's Autoscaler) to distinguish how many records went to each downstream."*

   It further documents the exact arithmetic invariant we observed for our topology type ("routed multi-output": filter + multi-sink fan-out):

   > *"For side outputs and multi-sink, the values partition the aggregate (**sum(values) == write-records**)."*

   This matches our measurements exactly: `Source write-records == NE-window read-records + Capacity-branch read-records` (e.g. 535 = 109 + 426; later samples: 8,086 ≈ 1,576 + 6,509).

4. **Why Flink shipped it this way**: the single `numRecordsOut` counter was designed assuming the common case of one output edge per task. It was never designed to distinguish per-downstream-edge counts, so once a stream fans out to multiple independent consumers, the aggregate silently becomes ambiguous. The Flink community considers this a real correctness gap (it even breaks the Kubernetes Operator's autoscaler), which is why FLIP-587 exists — to add a new `write-records-per-target` REST field that breaks the count down per downstream vertex.

5. **Practical takeaway (until FLIP-587 ships)**: trust the **downstream operator's own "Records Received"** for a branch that consumes the full, unfiltered stream (here, the capacity-alert branch) as the true total admission count — not the shared upstream vertex's aggregate "Records Sent," which double-counts records that are also sent down a second, filtered edge.

## References

- [FLIP-587: Expose per downstream target numRecordsOut metric](https://cwiki.apache.org/confluence/display/FLINK/FLIP-587%3A+Expose+per+downstream+target+numRecordsOut+metric)
- Draft PR: [apache/flink#28014](https://github.com/apache/flink/pull/28014)
- Related: [docs/data_skew.md](./data_skew.md)
