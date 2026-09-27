#  ProcessAllWindowFunction

The easiest way to understand `ProcessAllWindowFunction` is:

> **“When a window closes, give me all the records in that window, let me inspect the window itself, and let me produce whatever output I want.”**

This is for a **non-keyed window**—meaning there is no `keyBy()` before the window. That distinction is the most important thing to understand. 

---
# 1. Start with the big picture

Imagine this stream:

```text
10, 20, 30, 40, 50, 60, ...
```

Suppose we create **5-second tumbling windows**:

```text
Window 1: 0s ───── 5s
           10 20 30

Window 2: 5s ───── 10s
           40 50

Window 3: 10s ──── 15s
           60 70 80
```

With:

```java
stream
    .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
    .process(new MyProcessFunction());
```

Flink will essentially say:

> "The first 5-second window is ready. Here are **all the elements** that arrived in that window. What would you like to do with them?"

That's where `ProcessAllWindowFunction` comes in.

---

# 2. The basic structure

You write something like:

```java
public class MyWindowFunction
        extends ProcessAllWindowFunction<Integer, String, TimeWindow> {

    @Override
    public void process(
            Context context,
            Iterable<Integer> elements,
            Collector<String> out) {

        // process the elements
    }
}
```

There are three important parameters:

```java
process(
    Context context,
    Iterable<Integer> elements,
    Collector<String> out
)
```

Think of them as:

| Parameter  | Simple meaning                 |
| ---------- | ------------------------------ |
| `context`  | Information about the window   |
| `elements` | All records inside this window |
| `out`      | Where you send your results    |

The official API describes `elements` as the elements in the window and `context` as the context in which the window is evaluated. ([Apache Nightlies][1])

---

# 3. `elements` — the most important one

Suppose the window contains:

```text
10
20
30
40
```

Then:

```java
Iterable<Integer> elements
```

contains:

```text
10, 20, 30, 40
```

You can iterate through them:

```java
@Override
public void process(
        Context context,
        Iterable<Integer> elements,
        Collector<String> out) {

    for (Integer value : elements) {
        System.out.println(value);
    }
}
```

Output:

```text
10
20
30
40
```

So you can think of:

```java
elements
```

as:

> **"Give me everything that accumulated in this window."**

---

# 4. Example: calculate the average

Suppose every 5 seconds we receive:

```text
10
20
30
40
```

We want:

```text
Average = 25
```

We could write:

```java
public class AverageFunction
        extends ProcessAllWindowFunction<Integer, Double, TimeWindow> {

    @Override
    public void process(
            Context context,
            Iterable<Integer> elements,
            Collector<Double> out) {

        int sum = 0;
        int count = 0;

        for (Integer value : elements) {
            sum += value;
            count++;
        }

        if (count > 0) {
            out.collect((double) sum / count);
        }
    }
}
```

So:

```text
Window
----------------
10
20
30
40
----------------
        ↓
ProcessAllWindowFunction
        ↓
      25.0
```

---

# 5. `out` — how you produce results

This:

```java
Collector<Double> out
```

is basically your **output channel**.

You produce a result with:

```java
out.collect(result);
```

For example:

```java
out.collect(25.0);
```

You can actually produce **multiple results**.

For example:

```java
for (Integer value : elements) {
    if (value > 25) {
        out.collect((double) value);
    }
}
```

For:

```text
10
20
30
40
```

the output would be:

```text
30
40
```

The API explicitly allows the function to output **none, one, or several elements**. ([Apache Nightlies][1])

---

# 6. `Context` — the interesting part

Now we get to something `ProcessAllWindowFunction` gives you that a simple `ReduceFunction` doesn't:

```java
Context context
```

The context contains **information about the window**.

For example, you can get the window's timestamps.

Conceptually:

```java
context.window()
```

gives you the current window.

For a time window:

```text
10:00:00 ───────── 10:00:05
```

you can ask about the beginning and end of that window.

For example:

```java
TimeWindow window = context.window();

long start = window.getStart();
long end   = window.getEnd();
```

This becomes useful when you want to include window information in your output.

---

# 7. Example: produce a window report

Suppose the input is:

```text
10
20
30
40
```

and the window is:

```text
10:00:00 - 10:00:05
```

We might produce:

```text
Window 10:00:00 - 10:00:05
Count = 4
Average = 25
```

Code:

```java
public class WindowSummary
        extends ProcessAllWindowFunction<Integer, String, TimeWindow> {

    @Override
    public void process(
            Context context,
            Iterable<Integer> elements,
            Collector<String> out) {

        int count = 0;
        int sum = 0;

        for (Integer value : elements) {
            sum += value;
            count++;
        }

        TimeWindow window = context.window();

        double average = count == 0
                ? 0
                : (double) sum / count;

        out.collect(
            "Window: " + window.getStart()
            + " - " + window.getEnd()
            + ", count=" + count
            + ", average=" + average
        );
    }
}
```

This is one of the major reasons to use a `ProcessAllWindowFunction`: **you have both the data and metadata about the window.**

---

# 8. Why is it called `ProcessAll`?

This is where it can be confusing.

There are two different concepts:

### Keyed window

```java
stream
    .keyBy(sensor -> sensor.id)
    .window(...)
```

You get separate windows for each key:

```text
Sensor A
---------
20
21
22

Sensor B
---------
30
31
32
```

You would normally use:

```java
ProcessWindowFunction
```

---

### Non-keyed window

```java
stream
    .windowAll(...)
```

There is **one window containing everything**:

```text
Sensor A: 20
Sensor B: 30
Sensor A: 21
Sensor B: 31
Sensor A: 22
Sensor B: 32
```

Then:

```java
ProcessAllWindowFunction
```

processes that entire window.

So:

```text
keyBy()
   ↓
window()
   ↓
ProcessWindowFunction
```

versus:

```text
windowAll()
   ↓
ProcessAllWindowFunction
```

That's the key distinction.

---

# 9. A very practical example

Imagine an application monitoring temperatures from 1,000 sensors.

Events:

```java
SensorReading(
    "sensor-1", 72.5
)

SensorReading(
    "sensor-2", 68.2
)

SensorReading(
    "sensor-3", 75.1
)
```

Suppose you want:

> Every 10 seconds, calculate the **overall average temperature across all sensors**.

You don't want:

```text
sensor-1 → average
sensor-2 → average
sensor-3 → average
```

You want:

```text
ALL sensors
     ↓
10-second window
     ↓
overall average
```

That is a natural use case for:

```java
windowAll(...)
```

followed by:

```java
ProcessAllWindowFunction
```

---

# 10. Why not just use `ReduceFunction`?

This is an important question.

You could calculate an average using aggregation.

For example:

```java
windowAll(...)
    .aggregate(...)
```

That's generally more efficient because Flink doesn't need to retain all the elements merely to calculate an aggregate.

But `ProcessAllWindowFunction` gives you access to the **individual elements**:

```java
Iterable<IN> elements
```

So imagine you need to do something more complicated:

> "Look at every transaction in the 5-minute window, identify the top 10 transactions, calculate some statistics, and generate a report containing the window's start/end time."

That's where:

```java
ProcessAllWindowFunction
```

becomes attractive.

---

# 11. `clear()` — another method you'll see

The class also has:

```java
public void clear(Context context)
```

The purpose is to clean up state associated with the window when the window expires. The Flink 2.3 API specifically describes this in terms of the watermark passing the window's maximum timestamp plus allowed lateness. ([Apache Nightlies][1])

For example:

```java
@Override
public void clear(Context context) {
    // clean up any state you created
}
```

You don't always need to override it.

If you create additional state/resources associated with the window, this is where cleanup can be performed.

---

# 12. The mental model I recommend

For your Flink learning, I'd remember it like this:

```text
                    STREAM
                      │
                      ▼
                 windowAll()
                      │
                      ▼
             ┌─────────────────┐
             │     WINDOW      │
             │                 │
             │  A              │
             │  B              │
             │  C              │
             │  D              │
             └─────────────────┘
                      │
                      ▼
          ProcessAllWindowFunction
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Context     elements       out
          │           │           │
     window info   A,B,C,D     results
```

In plain English:

> **"Flink, when this non-keyed window is ready, give me the whole window, tell me about the window, and let me decide what output to produce."**

---

## One final comparison

This is probably the most useful distinction to keep in your head:

```text
ProcessFunction
    │
    └── Process one event at a time


KeyedProcessFunction
    │
    └── Process one event at a time,
        with keyed state/timers


ProcessWindowFunction
    │
    └── Process ALL events in a window
        for EACH KEY


ProcessAllWindowFunction
    │
    └── Process ALL events in a window
        with NO KEY
```

And the corresponding Flink setup is:

```java
// One window per key
stream
    .keyBy(...)
    .window(...)
    .process(new ProcessWindowFunction<>());
```

versus:

```java
// One window for the entire stream
stream
    .windowAll(...)
    .process(new ProcessAllWindowFunction<>());
```

That **`keyBy()` vs `windowAll()` distinction** is the single most important thing to understand before moving on to `ProcessWindowFunction`. ([Apache Nightlies][1])

[1]: https://nightlies.apache.org/flink/flink-docs-release-2.3/api/java/org/apache/flink/streaming/api/functions/windowing/ProcessAllWindowFunction.html "ProcessAllWindowFunction (Flink : 2.3-SNAPSHOT API)"
