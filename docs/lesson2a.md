# Lesson 2A: Understanding the Flink Job Graph

## Question

Can you explain this Job Graph in simple terms? How did the Stream Graph get converted to this?

## Response

Here is a breakdown of what this **JobGraph** represents in simple terms, followed by how Flink converted your code's **StreamGraph** into this.

---

### 1. The Big Picture: What the Job Graph Shows

Your `Lesson2A.java` program creates one data stream of admissions and splits it into **two independent pipelines**:

```text
                                +---> [windowAll (Parallelism: 1)] ---> [Sink: print (Parallelism: 2)]
                                |     (Network Total)
[Source (Parallelism: 2)] ------+
                                |
                                +---> [keyBy + Window -> Sink (Chained, Parallelism: 2)]
                                      (Per-Region Breakdown)
```

#### Node 1: `Source: patient-admissions-source` (Parallelism: 2)

- **What it does**: Two parallel generator tasks produce patient admission records (`EmergencyRoom` objects) at 2 events/sec.
- **Why it branches**: The single output stream `admissionsStream` is consumed by two downstreams: `measureAcrossAllRegions` and `measureByRegion`.

#### Branch A (Top): Total Across All 4 Regions

- **Node 2 (`TriggerWindow...`, Parallelism: 1)**:
  - This is `windowAll(...)`. Because you asked for a single total across **all regions**, Flink cannot process this in parallel across multiple machines—**all** events from both source workers must be funneled into a **single worker** (Parallelism = 1) so it can count the network-wide total.
  - Notice the incoming edge is **`HASH`** (or forward/partition): both source tasks route their records into this single task.
- **Node 3 (`Sink: print-all-regions-admissions`, Parallelism: 2)**:
  - Prints the window output to stdout.
  - Notice the edge between Node 2 and Node 3 is **`REBALANCE`**: 1 task is sending output to 2 sink tasks using round-robin distribution.

#### Branch B (Bottom): Per-Region Counts

- **Node 4 (`TumblingProcessingTimeWindows -> Sink: print-regional-admissions`, Parallelism: 2)**:
  - This is `keyBy(EmergencyRoom::getRegionID)`.
  - Because records are keyed by `regionID`, Flink hashes the region ID (`NE`, `MW`, `W`, `S`) across the 2 parallel workers.
  - Worker 1 might handle `NE` and `MW`, while Worker 2 handles `W` and `S`.
  - **Operator Chaining**: Flink noticed that the window operator and the print sink share the same parallelism (2) and a 1-to-1 forward connection, so it **fused them into a single vertex** (`TumblingProcessingTimeWindows -> Sink`).

---

### 2. How the StreamGraph got converted to this JobGraph

When you call `env.execute()`, Flink goes through this transformation pipeline:

```text
Java Code (DataStream API)
       │
       ▼
1. StreamGraph    (Logical view: 1:1 with your method calls)
       │
       ▼ [Optimization: Operator Chaining & Partitioning]
2. JobGraph       (Execution units: what you see in the Web UI)
       │
       ▼
3. ExecutionGraph (Physical deployment on TaskManagers/threads)
```

#### Step 1: The Logical StreamGraph (Before Optimization)

In the raw `StreamGraph`, every API call is a separate node:

1. `Source`
2. `AllWindow` (from `windowAll()`)
3. `Sink: print-all-regions`
4. `KeyBy`
5. `KeyedWindow` (from `keyBy().window()`)
6. `Sink: print-regional`

Total: **6 distinct operator nodes**.

---

#### Step 2: The JobGraph Optimizer (Operator Chaining)

To avoid unnecessary network transfer, serialization, and thread switching, Flink merges adjacent operators into a single task whenever possible (**Operator Chaining**).

For two operators to chain together:

1. They must have the **exact same parallelism**.
2. The data transfer between them must be **FORWARD** (local in-memory handoff, no shuffle/hash/rebalance).
3. They must be in the same slot sharing group.

Let's see how each part was evaluated:

| Operators in StreamGraph | Can they chain? | Why / Why not? | Result in JobGraph |
|---|---|---|---|
| **Source** $\to$ **AllWindow** | ❌ No | Source has `Parallelism: 2`, AllWindow requires `Parallelism: 1`. Data must cross threads/nodes. | Distinct vertex |
| **AllWindow** $\to$ **Print Sink** | ❌ No | AllWindow has `Parallelism: 1`, Print Sink has `Parallelism: 2`. Requires `REBALANCE`. | Distinct vertex |
| **Source** $\to$ **KeyedWindow** | ❌ No | Requires network shuffle by key hash (`keyBy(regionID)`). | Distinct vertex |
| **KeyedWindow** $\to$ **Print Sink** | ✅ **YES** | Both have `Parallelism: 2`, and data flows directly from window task 1 to sink task 1 without re-partitioning. | **Chained into 1 Vertex** |

Because of this, the 6 logical operators in the StreamGraph collapsed into the **4 execution vertices** visible on your screen.

---

### 3. Reading the Metrics Table

At the moment of the screenshot (runtime: ~3m 32s):

- **Source**: Generated **418 records** across its 2 tasks (~2 records/sec for ~210 seconds).
- **TriggerWindow (All Regions)**:
  - Received: **418 records**
  - Sent: **2 records** (each record represents one fired 2-minute tumbling window!).
- **Sink (All Regions)**: Received and printed those **2 window summaries**.
- **Keyed Window + Sink (Bottom)**: Received **418 records** partitioned across the 2 tasks and printed the regional counts upon window triggers.

---

### Related implementation

- [Lesson2A.java](../src/main/java/org/muralis/flink/launcher/Lesson2A.java)
- [HospitalAdmissionAnalytics.java](../src/main/java/org/muralis/flink/data/source/hospital/HospitalAdmissionAnalytics.java)
- [PatientAdmissionSource.java](../src/main/java/org/muralis/flink/data/source/hospital/PatientAdmissionSource.java)
