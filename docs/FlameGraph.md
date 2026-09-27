# FlameGraph
Based on the screenshot from your **Apache Flink Dashboard** for the running job **`Hospital Patient Admissions 2-Minute Window Measurement`**, here is how to interpret this specific FlameGraph and why it is useful.

---

## 1. How to Read a FlameGraph

* **X-Axis (Width = Proportion of Time):** The width of a box indicates the percentage of sampled stack traces in which that frame was present. It does **not** represent sequential time from left to right. A wide box means execution spent a significant fraction of time in that method or its children.


* **Y-Axis (Height = Call Stack Depth):** The vertical hierarchy shows stack depth. The bottom (`root` / `java.lang.Thread.run`) is the caller, and methods call upwards toward the top.


* **Colors:** Flink uses a color spectrum (yellow $\rightarrow$ orange $\rightarrow$ red) to highlight execution intensity and frame depth, with red frames at the top typically representing leaf executions or active thread state.



---

## 2. Interpreting Your Specific FlameGraph

Looking at the selected subtask's FlameGraph (Subtask 1, **Mixed** Mode):

```text
jdk.internal.misc.Unsafe.park
  ▲
java.util.concurrent.locks.LockSupport.parkNanos
  ▲
java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await
  ▲
org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.processMail
  ▲
...
org.apache.flink.runtime.taskmanager.Task.run
  ▲
java.lang.Thread.run

```

### Key Observation: The Task is Currently Idle / Waiting

1. **Top Leaf Frames:** The execution stack terminates in `Unsafe.park` and `LockSupport.parkNanos` via `AbstractQueuedSynchronizer`. The hover tooltip shows **`100.000%, 100 samples`**.


2. **Flink Mailbox Loop:** The stack goes through `MailboxProcessor.runMailboxLoop` and `processMail`.


3. **What it means:** Flink's Mailbox Processor is parked waiting for incoming records or scheduled timer events. Because 100% of samples are waiting on condition locks in a `Mixed` or `Off-CPU` view, this subtask is **idle** (waiting for data from upstream or waiting for the processing-time window to fire).



This aligns with the DAG metrics on the left showing **`Busy (max): 0%`** and **`Backpressured (max): 0%`**.

---

## 3. Why FlameGraphs Are Useful in Flink

1. **Pinpointing CPU Bottlenecks (On-CPU Mode):** Switching the toggle to **On-CPU** filters out parked/waiting threads. Wide boxes on top will pinpoint exact lines of code (e.g., inefficient serialization, regex parsing, nested loops) consuming CPU cycles.


2. **Diagnosing I/O and Lock Contention (Off-CPU Mode):** Toggling to **Off-CPU** or **Mixed** reveals thread blocking, network I/O delays, synchronous database calls, or lock contention that stalls stream processing.


3. **Optimizing Hot Path Methods:** Instead of guessing where time is spent in complex window operations or custom functions, FlameGraphs visually direct optimization efforts to the widest methods.


4. **Troubleshooting Backpressure:** When a task is backpressured or busy, comparing FlameGraphs across subtasks quickly identifies if a performance degradation is caused by user-code logic or underlying framework overhead.