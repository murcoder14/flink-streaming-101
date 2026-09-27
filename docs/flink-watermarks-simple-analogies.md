# Flink Watermarks Explained with Simple Analogies

## The Question

Can you explain this Flink code in simple language using analogies?

```java
// Watermarks tell Flink how far event time has progressed
// ("no reading older than X will arrive any more").
// Only the timer lesson needs them. Our readings are created in timestamp order,
// so the simplest strategy, "monotonously increasing timestamps", is enough.
// The assigner says where the event time is stored.
WatermarkStrategy<SensorReading> watermarks = WatermarkStrategy
        .<SensorReading>forMonotonousTimestamps()
        .withTimestampAssigner((reading, previousTimestamp) -> reading.timestamp);
```

---

# The Big Picture

Think of this code as giving Flink **a clock and a promise about the order in which sensor readings arrive**.

The code is essentially telling Flink:

> **"Use the `timestamp` field in each `SensorReading` as its event time. I promise that these timestamps will arrive in increasing order, so you can use the monotonically-increasing timestamp watermark strategy."**

---

# 1. What Problem Are We Solving?

Suppose a sensor produces these readings:

```text
Reading       Event Time
-------       ----------
A             10:00:01
B             10:00:02
C             10:00:03
D             10:00:04
```

The timestamp inside the reading represents **when the event actually happened**.

That is called **event time**.

But Flink does not automatically know which field contains that timestamp.

So we need to tell Flink:

> "Use the `timestamp` field in each `SensorReading` as the event-time timestamp."

---

# 2. What Is a Watermark?

Imagine packages being delivered.

Each package has a label saying when it was originally shipped:

```text
Package A → Jan 1
Package B → Jan 2
Package C → Jan 3
```

The delivery truck may not deliver them immediately.

You want to process the packages according to their **shipping date**, not according to when they physically arrive.

Eventually, you might say:

> "I've now seen everything through Jan 3. I don't expect a package with a shipping date earlier than Jan 3 anymore."

That is the basic idea behind a **watermark**.

In Flink:

```text
Watermark = "I believe I have seen everything up to this event time."
```

For example:

```text
Watermark = 10:00:05
```

means roughly:

> "Flink believes that events with timestamps earlier than 10:00:05 should no longer arrive."

Watermarks are particularly important for **event-time windows and timers**.

---

# 3. Why Does Flink Need Watermarks?

Consider a 10-second event-time window:

```text
10:00:00 ───────────────── 10:00:10
```

Suppose Flink receives:

```text
10:00:01
10:00:04
10:00:07
```

Should Flink close the window?

Not necessarily.

Maybe this event is still coming:

```text
10:00:03
```

The event could simply be late.

Flink needs some way of knowing:

> "Are we still waiting for events belonging to this window?"

The watermark provides that information.

When the watermark passes `10:00:10`:

```text
10:00:00 ───────────────── 10:00:10
                            ↑
                        watermark
```

Flink can say:

> "Okay, we're done waiting for this window."

It can then fire the appropriate window operation or timer.

---

# 4. What Does `forMonotonousTimestamps()` Mean?

This is the important part:

```java
.forMonotonousTimestamps()
```

**Monotonous** basically means:

> The timestamps never go backward.

For example:

```text
10:00:01
10:00:02
10:00:03
10:00:04
10:00:05
```

Good.

But:

```text
10:00:01
10:00:03
10:00:02
10:00:04
```

is **not** monotonically increasing because `10:00:02` arrived after `10:00:03`.

### Analogy: Theater Tickets

Imagine people entering a theater carrying numbered tickets.

You tell the usher:

> "People will always arrive with ticket numbers increasing."

If ticket #100 has arrived, the usher doesn't expect ticket #50 to suddenly show up.

That is essentially the assumption you're making with:

```java
forMonotonousTimestamps()
```

You're telling Flink:

> **"My events arrive with timestamps in increasing order."**

---

# 5. What Does `.withTimestampAssigner()` Do?

Suppose your object looks like this:

```java
public class SensorReading {
    String sensorId;
    long timestamp;
    double temperature;
}
```

For example:

```text
SensorReading
--------------------------------
sensorId     = "S1"
timestamp    = 1005
temperature  = 72.4
```

Flink needs to know:

> "Which field represents event time?"

You tell it:

```java
.withTimestampAssigner(
    (reading, previousTimestamp) -> reading.timestamp
)
```

In plain English:

> **"When you receive a `SensorReading`, use its `timestamp` field as its event-time timestamp."**

So:

```text
SensorReading
      |
      v
timestamp = 1005
      |
      v
Flink event time = 1005
```

---

# 6. What Is `previousTimestamp`?

The timestamp assigner receives two values:

```java
reading
previousTimestamp
```

`reading` is the current sensor reading.

`previousTimestamp` is the timestamp Flink previously assigned.

But this particular code doesn't need it.

So it simply ignores it:

```java
(reading, previousTimestamp) -> reading.timestamp
```

This means:

> "I don't care what the previous timestamp was. Just take the timestamp from the current reading."

---

# 7. Putting Everything Together

This:

```java
WatermarkStrategy<SensorReading> watermarks = WatermarkStrategy
        .<SensorReading>forMonotonousTimestamps()
        .withTimestampAssigner(
            (reading, previousTimestamp) -> reading.timestamp
        );
```

can be translated into English as:

> **"Flink, use the `timestamp` field in each `SensorReading` as its event time. I promise that these timestamps will arrive in increasing order, so you can use the monotonically-increasing timestamp watermark strategy."**

That's the essence of the code.

---

# 8. Three Concepts You Should Not Confuse

There are three different clocks/concepts:

```text
Event timestamp
      ↓
"When did this event actually happen?"

Watermark
      ↓
"How far do we believe event time has progressed?"

Processing time
      ↓
"When did Flink actually receive/process this event?"
```

For example:

```text
Sensor reading says:
timestamp = 10:00:01

But Flink receives it at:
10:00:05
```

Then:

```text
Event time      = 10:00:01
Processing time = 10:00:05
```

The watermark is Flink's way of saying:

```text
"I believe I've received everything up through approximately X."
```

---

# 9. A Final Picture

Think of Flink as watching a train of events:

```text
Events arriving:

10:00:01 → 10:00:02 → 10:00:03 → 10:00:04 → 10:00:05
                                                   ↑
                                               Watermark
```

Because you've told Flink:

```java
forMonotonousTimestamps()
```

Flink can confidently move the watermark forward as the timestamps advance.

---

# 10. What If Events Can Arrive Out of Order?

Suppose your real system produces:

```text
10:00:01
10:00:05
10:00:02   ← late/out-of-order
10:00:06
```

Then:

```java
forMonotonousTimestamps()
```

is **not appropriate**.

You would generally use a strategy such as:

```java
forBoundedOutOfOrderness(...)
```

This tells Flink:

> **"Events can arrive out of order, but only by up to approximately N seconds."**

That distinction is one of the most important concepts for understanding Flink watermarks.

---

# Quick Mental Model

Remember this:

```text
Timestamp Assigner
        ↓
"What time did this event happen?"

Watermark
        ↓
"How far have we progressed through event time?"

forMonotonousTimestamps()
        ↓
"Events arrive in timestamp order."

forBoundedOutOfOrderness()
        ↓
"Events may arrive late/out of order,
 but only within a known tolerance."
```

If you remember those four statements, you have the foundation for understanding Flink event-time processing.
