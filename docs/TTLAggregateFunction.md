In Apache Flink, **`org.apache.flink.runtime.state.v2.ttl.TtlAggregateFunction`** is part of **Flink's State V2 API**.

**State V2 is Flink's newer keyed-state API designed to support asynchronous, non-blocking state access.** TTL and asynchronous state are **separate capabilities** that happen to coexist in State V2. For example:

```java
state.asyncValue()
```

returns a `StateFuture`, rather than blocking until the state has been retrieved.

The asynchronous API is designed to be more efficient and powerful, and recommends it where possible. A `KeyedStream` must have `enableAsyncState()` enabled to use the new State V2 API. 

When working with state in real-world streaming pipelines, you often want state entries (such as aggregated metrics) to automatically expire after a specified **Time-To-Live (TTL)**. While users usually configure TTL via `StateTtlConfig` on state descriptors, Flink uses wrapper classes like `TtlAggregateFunction` internally (or when wrapping custom `AggregateFunction` instances) to enforce expiry mechanics directly during state reads and updates.

# What does `TtlAggregateFunction` actually do?

Suppose your application defines:

```java
AggregateFunction<Transaction, Accumulator, Double>
```

Your normal aggregation might be:

```text
Transaction
     │
     ▼
Accumulator
     │
     ├── sum
     └── count
     │
     ▼
Average
```

With TTL, Flink can internally wrap that function:

```text
                  Your AggregateFunction
                          │
                          ▼
                 TtlAggregateFunction
                          │
                 ┌────────┴────────┐
                 │                 │
          aggregation logic    TTL metadata
                 │                 │
                 └────────┬────────┘
                          ▼
                     State V2
```

The actual API shows that the TTL wrapper operates on:

```java
TtlValue<ACC>
```

rather than simply `ACC`. Flink internally wraps the accumulator in a TTL-aware representation containing the user value and TTL timestamp metadata. And importantly, the TTL timestamp represents the relevant **last access/update time according to the configured TTL semantics**, not simply "creation time."

# TTL is not "delete after 24 hours"

This is one of the most important corrections.

Suppose:

```java
TTL = 24 hours
UpdateType = OnCreateAndWrite
```

and user `A` has:

```text
10:00 AM → transaction $500
```

The state becomes:

```text
user A → $500
expires based on TTL timestamp
```

If another transaction arrives:

```text
8:00 PM → $200
```

the write refreshes the TTL.

So the expiration is effectively extended.

```text
10 AM
 │
 ├── $500
 │
 │       8 PM
 │        │
 │        ├── $200
 │        │
 │        ▼
 │     TTL refreshed
 │
 └──────────────────────→ expiration
```

Flink's `OnCreateAndWrite` semantics refresh TTL on creation and write. `OnReadAndWrite` also refreshes it on reads. Therefore, **Expire the keyed state after 24 hours without a qualifying state update.**


Here are **3 real-world streaming examples** that illustrate where state expiry on aggregations is critical, along with Java code demonstrating how `StateTtlConfig` wraps an `AggregateFunction` in practice.

---

### Real-World Use Cases

#### 1. Real-Time Fraud Detection: Sliding Transaction Rolling Sums

* **Scenario:** An e-commerce system tracks high-frequency credit card attempts per user. If a user spends over $10,000 in a rolling window, a fraud alert triggers.
* **Why TTL matters:** If a user makes transactions and then goes inactive, their accumulated amount must expire (e.g., after 24 hours of inactivity). Without state TTL, millions of inactive user keys remain in RocksDB forever, degrading performance and causing memory leaks.

#### 2. Active User Engagement: Daily Session Aggregations

* **Scenario:** A gaming or social media app counts total active user interactions (likes, shares, clicks) per user ID using an `AggregatingState`.
* **Why TTL matters:** Active users continuously update their aggregate state. If a user drops off, setting a TTL (e.g., 3 days) ensures the user state automatically purges from state memory once the session naturally expires without explicit deletion logic.

#### 3. IoT Anomaly Detection: Temperature Rolling Averages

* **Scenario:** Millions of industrial sensors emit temperature readings every few seconds. An aggregation function maintains `(Sum, Count)` per sensor to calculate moving averages.
* **Why TTL matters:** Decommissioned or broken sensors stop producing events. A TTL (e.g., 1 hour) cleans up orphaned sensor keys automatically on the stream processor level.

---

### Java Implementation Example

In Flink Streaming applications, you construct state with TTL using `StateTtlConfig` and pass your user-defined `AggregateFunction` into state descriptors (such as `AggregatingStateDescriptor` or `ListStateDescriptor`). Under the hood, Flink wraps your aggregation in internal TTL classes like `TtlAggregateFunction` to transparently intercept reads and update creation timestamps.

```java
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class FraudDetectionAggregator extends RichFlatMapFunction<TransactionEvent, Double> {

    // Aggregating state to store the rolling spend total per user
    private transient AggregatingState<Double, Double> userRollingSpendState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 1. Define the TTL Configuration for State Expiry
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(24)) // Expire state 24 hours after last update
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000) // Efficient background cleanup
                .build();

        // 2. Define the AggregateFunction (Accumulator logic)
        AggregateFunction<Double, Double, Double> sumAggregator = new AggregateFunction<>() {
            @Override
            public Double createAccumulator() {
                return 0.0;
            }

            @Override
            public Double add(Double value, Double accumulator) {
                return accumulator + value;
            }

            @Override
            public Double getResult(Double accumulator) {
                return accumulator;
            }

            @Override
            public Double merge(Double a, Double b) {
                return a + b;
            }
        };

        // 3. Create the AggregatingStateDescriptor and enable TTL
        AggregatingStateDescriptor<Double, Double, Double> descriptor =
                new AggregatingStateDescriptor<>(
                        "user-rolling-spend",
                        sumAggregator,
                        Types.DOUBLE
                );

        // Flink uses TtlAggregateFunction internally to manage state TTL for this descriptor
        descriptor.enableTimeToLive(ttlConfig);

        userRollingSpendState = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(TransactionEvent value, Collector<Double> out) throws Exception {
        // Add transaction amount to state; automatically updates state TTL timestamp
        userRollingSpendState.add(value.getAmount());

        Double currentSpendTotal = userRollingSpendState.get();

        // Check fraud threshold ($10,000)
        if (currentSpendTotal != null && currentSpendTotal > 10000.0) {
            out.collect(currentSpendTotal);
        }
    }
}

```

---

### What `TtlAggregateFunction` Handles Under the Hood

When TTL is enabled on an `AggregatingState`, Flink decorates your aggregation instance with a TTL wrapper (like `TtlAggregateFunction`). It enforces three core behaviors:

1. **Timestamp Wrapping:** Pairs your accumulator value with a creation/modification timestamp `(Accumulator, LastAccessTimestamp)`.
2. **Expired Value Suppression:** When `.get()` or `.getResult()` is called, it checks `currentTime - LastAccessTimestamp > TTL`. If expired, it resets or returns `null` based on your `StateVisibility` settings.
3. **Automatic Purging:** During compaction (e.g., in RocksDB), expired aggregate accumulators are automatically dropped from disk storage without requiring manual state cleanup timers.