#  ProcessWindowFunction
 `ProcessWindowFunction` is very similar to the `ProcessAllWindowFunction` with **one crucial difference**:

> **`ProcessWindowFunction` works on a window for each key created by `keyBy()`. It processes all the records belonging to a specific key in the particular window.**

The Flink 2.3 API describes it as a function evaluated over **keyed (grouped) windows**. Its `process()` method receives the key, window context, all elements in that window, and an output collector. ([Apache Nightlies][1])

---

# 1. Start with the simplest example

Suppose we have temperature readings:

```text
Sensor A → 70
Sensor B → 80
Sensor A → 72
Sensor B → 82
Sensor A → 74
Sensor B → 84
```

We want to calculate something every 5 seconds.

First we do:

```java
stream
    .keyBy(reading -> reading.sensorId)
    .window(...)
    .process(new MyWindowFunction());
```

The important part is:

```java
.keyBy(reading -> reading.sensorId)
```

This tells Flink:

> "Separate the stream into groups based on sensor ID."

So Flink conceptually creates:

```text
             STREAM
                |
             keyBy()
                |
       +--------+--------+
       |                 |
    Sensor A          Sensor B
       |                 |
   70, 72, 74         80, 82, 84
       |                 |
       +--------+--------+
                |
             windows
                |
       ProcessWindowFunction
```

---

# 2. What does `ProcessWindowFunction` actually receive?

The function looks like this:

```java
public class MyWindowFunction
        extends ProcessWindowFunction<
            SensorReading,
            String,
            String,
            TimeWindow> {

    @Override
    public void process(
            String key,
            Context context,
            Iterable<SensorReading> elements,
            Collector<String> out)
            throws Exception {

        // do something
    }
}
```

There are **four important things** passed to `process()`:

```java
process(
    key,
    context,
    elements,
    out
)
```

Think of them as:

| Parameter  | Meaning                                 |
| ---------- | --------------------------------------- |
| `key`      | Which group are we processing?          |
| `context`  | Information about the window            |
| `elements` | All records in this window for this key |
| `out`      | Where we send our result                |

This is exactly how the Flink 2.3 API defines those parameters. ([Apache Nightlies][1])

---

# 3. The `key` parameter

This is perhaps the biggest difference from `ProcessAllWindowFunction`.

Suppose:

```java
.keyBy(reading -> reading.sensorId)
```

and the window contains:

```text
Sensor A → 70
Sensor A → 72
Sensor A → 74
```

Then Flink calls:

```java
process(...)
```

with:

```text
key = "Sensor A"
```

And separately it will call the function for:

```text
key = "Sensor B"
```

So conceptually:

```text
Window 1
────────────────────────

Sensor A
  70
  72
  74
       ↓
ProcessWindowFunction
       ↓
key = "Sensor A"


Sensor B
  80
  82
  84
       ↓
ProcessWindowFunction
       ↓
key = "Sensor B"
```

**The function is not processing all sensors together.**

It processes **one key's window at a time**.

---

# 4. `elements`

This is:

```java
Iterable<SensorReading> elements
```

For Sensor A, perhaps:

```text
70
72
74
```

For Sensor B:

```text
80
82
84
```

You can iterate through them:

```java
for (SensorReading reading : elements) {
    System.out.println(reading);
}
```

So `elements` means:

> **"Give me all the records belonging to this key in this particular window."**

---

# 5. A complete simple example

Let's calculate the average temperature for each sensor.

```java
public class AverageTemperature
        extends ProcessWindowFunction<
            SensorReading,
            String,
            String,
            TimeWindow> {

    @Override
    public void process(
            String sensorId,
            Context context,
            Iterable<SensorReading> readings,
            Collector<String> out)
            throws Exception {

        double sum = 0;
        int count = 0;

        for (SensorReading reading : readings) {
            sum += reading.temperature;
            count++;
        }

        double average = sum / count;

        out.collect(
            sensorId + " average = " + average
        );
    }
}
```

Suppose our window contains:

```text
Sensor A → 70
Sensor A → 72
Sensor A → 74

Sensor B → 80
Sensor B → 82
Sensor B → 84
```

Flink effectively does:

```text
key = A
elements = [70, 72, 74]
             ↓
         average = 72

key = B
elements = [80, 82, 84]
             ↓
         average = 82
```

Output:

```text
Sensor A average = 72
Sensor B average = 82
```

---

# 6. Why do we need `context`?

This is where `ProcessWindowFunction` becomes particularly useful.

The `Context` gives you **information about the window**.

For example:

```java
TimeWindow window = context.window();
```

Then:

```java
window.getStart()
window.getEnd()
```

can tell you the window's boundaries.

So you could produce:

```text
Sensor A
Window: 10:00:00 - 10:00:05
Average: 72
```

For example:

```java
TimeWindow window = context.window();

out.collect(
    sensorId
    + " | window="
    + window.getStart()
    + "-"
    + window.getEnd()
    + " | average="
    + average
);
```

---

# 7. Why not just use `AggregateFunction`?

This is an important Flink concept.

Suppose all you want is:

```text
Sensor A → average
Sensor B → average
```

You could use an aggregation function.

That's often preferable because Flink can maintain a compact accumulator rather than keeping all the records around.

But sometimes you want to look at **all the records in the window**.

For example:

> "For every sensor, give me the top 3 highest temperatures in each 5-minute window."

Now having:

```java
Iterable<SensorReading> elements
```

is very useful.

You can do:

```java
List<SensorReading> readings = new ArrayList<>();

for (SensorReading reading : elements) {
    readings.add(reading);
}

readings.sort(
    Comparator.comparingDouble(r -> r.temperature)
              .reversed()
);
```

Then:

```text
Sensor A
----------------
78
76
75
72
70

Top 3:
78
76
75
```

and output those three.

---

# 8. Here's the key difference from `ProcessAllWindowFunction`

This is worth memorizing.

### `ProcessAllWindowFunction`

You have:

```java
stream
    .windowAll(...)
    .process(...)
```

There is **no key**.

Imagine:

```text
Window
────────────────────
Sensor A: 70
Sensor A: 72
Sensor B: 80
Sensor B: 82
Sensor C: 90
────────────────────
        ↓
ProcessAllWindowFunction
```

The function sees:

```text
70, 72, 80, 82, 90
```

as one collection.

---

### `ProcessWindowFunction`

You have:

```java
stream
    .keyBy(...)
    .window(...)
    .process(...)
```

Now Flink creates separate windows per key:

```text
             Window
               |
       +-------+-------+
       |       |       |
       A       B       C
       |       |       |
     70,72   80,82     90
       |       |       |
       ↓       ↓       ↓
    process process process
```

That's the fundamental distinction.

---

# 9. An analogy

Imagine a school.

You have:

```text
100 students
```

### `ProcessAllWindowFunction`

You say:

> "Give me **all students** who attended this class."

You get:

```text
Student 1
Student 2
Student 3
...
Student 100
```

You process everyone together.

---

### `ProcessWindowFunction`

You first say:

```java
.groupBy(student -> student.grade)
```

Now you have:

```text
Grade 9
Grade 10
Grade 11
Grade 12
```

Then the window function gets called separately:

```text
ProcessWindowFunction
        ↓
Grade 9 students

ProcessWindowFunction
        ↓
Grade 10 students

ProcessWindowFunction
        ↓
Grade 11 students

ProcessWindowFunction
        ↓
Grade 12 students
```

That is essentially what `keyBy()` does.

---

# 10. The complete flow

This is the mental picture I'd use:

```text
                 EVENTS
                   |
                   v
                keyBy()
                   |
        +----------+----------+
        |          |          |
        v          v          v
     KEY = A    KEY = B    KEY = C
        |          |          |
        v          v          v
     WINDOW     WINDOW     WINDOW
        |          |          |
        +----------+----------+
                   |
                   v
       ProcessWindowFunction
                   |
          +--------+--------+
          |        |        |
          v        v        v
         key    context   elements
          |        |        |
          |        |        |
          |     window      |
          |     info        |
          |                 |
          +--------+--------+
                   |
                   v
                 out
```

---

# 11. What happens when the window closes?

Suppose we have a **5-second tumbling window**:

```text
0s                    5s
|----------------------|
```

For Sensor A:

```text
0.5s → 70
1.2s → 72
3.7s → 74
```

Sensor B:

```text
0.8s → 80
2.5s → 82
4.1s → 84
```

When the window is evaluated, Flink effectively has:

```text
A → [70, 72, 74]
B → [80, 82, 84]
```

Then:

```text
ProcessWindowFunction
        |
        +--> key=A
        |      elements=[70,72,74]
        |
        +--> key=B
               elements=[80,82,84]
```

The function runs separately for A and B.

---

# 12. `clear()`

The other method you will see is:

```java
@Override
public void clear(Context context) {
    // cleanup
}
```

The Flink API says this is used to delete state in the context when the window expires—specifically when the watermark passes the window's maximum timestamp plus allowed lateness. ([Apache Nightlies][1])

You don't necessarily need to implement it.

But if your function creates additional state associated with a window, `clear()` is where you can clean it up.

---

# 13. One subtle but important point

`ProcessWindowFunction` is **not** the same as:

```java
KeyedProcessFunction
```

They operate at different levels.

### `KeyedProcessFunction`

```text
Event 1 → process()
Event 2 → process()
Event 3 → process()
Event 4 → process()
```

It processes **one event at a time**.

You can use:

* keyed state
* timers
* event-by-event logic

---

### `ProcessWindowFunction`

```text
Window
-----------------
Event 1
Event 2
Event 3
Event 4
-----------------
        ↓
    process()
```

It processes **the window as a whole**.

That's a very useful distinction.

---

# 14. Compare the four functions you've encountered

Given what you've been learning about Flink, I'd organize them like this:

| Function                   | Input to function    |  Keyed? | Window? |
| -------------------------- | -------------------- | ------: | ------: |
| `ProcessFunction`          | One event            |      No |      No |
| `KeyedProcessFunction`     | One event            | **Yes** |      No |
| `ProcessAllWindowFunction` | All events in window |      No | **Yes** |
| `ProcessWindowFunction`    | All events in window | **Yes** | **Yes** |

And visually:

```text
                    EVENT
                      |
          +-----------+-----------+
          |                       |
       no key                   keyBy()
          |                       |
          v                       v
 ProcessFunction       KeyedProcessFunction
   one event               one event
          
          
                    WINDOW
                      |
          +-----------+-----------+
          |                       |
      windowAll()               keyBy()
          |                       |
          v                       v
ProcessAllWindowFunction   ProcessWindowFunction
   entire window          window for ONE key
```

### The one sentence to remember

> **`ProcessWindowFunction` means: "For each key, when its window is ready, give me all the records in that window plus information about the window, and let me decide what to output."** ([Apache Nightlies][1])

If you understand that sentence, you understand the core purpose of `ProcessWindowFunction`.

[1]: https://nightlies.apache.org/flink/flink-docs-release-2.3/api/java/org/apache/flink/streaming/api/functions/windowing/ProcessWindowFunction.html "ProcessWindowFunction (Flink : 2.3-SNAPSHOT API)"
