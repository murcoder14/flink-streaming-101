# Using FlatMap in Streaming

In Flink, `flatMap()` is particularly useful in streaming because **one incoming event can produce zero, one, or many outgoing events**.

A simple mental model:

```text
map:
    1 input  →  exactly 1 output

filter:
    1 input  →  0 or 1 output

flatMap:
    1 input  →  0, 1, or MANY outputs
```

## Realistic streaming scenarios

---

## 1. Split a log message into multiple words

Suppose your stream contains:

```text
"Apache Flink is fast"
"Streaming is powerful"
```

You want a stream of individual words.

```java
DataStream<String> words = logs.flatMap(
    (String line, Collector<String> out) -> {
        for (String word : line.split(" ")) {
            out.collect(word);
        }
    }
);
```

Input:

```text
"Apache Flink is fast"
```

Output:

```text
Apache
Flink
is
fast
```

### Why `flatMap`?

One input event produces **four events**.

```text
"Apache Flink is fast"
          |
       flatMap
          |
    +-----+-----+-----+-----+
    |     |     |     |     |
 Apache Flink  is   fast
```

This is one of the classic examples.

---

# 2. Parse an event containing multiple transactions

Imagine an upstream system sends:

```json
{
  "customerId": "C101",
  "transactions": [
    {"product": "Book", "amount": 25},
    {"product": "Pen", "amount": 5},
    {"product": "Laptop", "amount": 1200}
  ]
}
```

Your downstream processing wants **one transaction per event**.

```java
stream.flatMap((Order order, Collector<Transaction> out) -> {

    for (Transaction transaction : order.getTransactions()) {
        out.collect(transaction);
    }

});
```

One order:

```text
Order C101
   |
   | flatMap
   |
   +----> Book $25
   +----> Pen $5
   +----> Laptop $1200
```

This is very common in real streaming applications.

---

# 3. Filter malformed events while parsing

`flatMap` can produce **zero events**.

Suppose Kafka contains:

```text
101,John,25
102,Mary,31
INVALID
103,Bob,42
```

You want valid `Person` objects.

```java
stream.flatMap((String line, Collector<Person> out) -> {

    String[] fields = line.split(",");

    if (fields.length == 3) {
        out.collect(
            new Person(
                Integer.parseInt(fields[0]),
                fields[1],
                Integer.parseInt(fields[2])
            )
        );
    }

});
```

For:

```text
INVALID
```

nothing is emitted.

So:

```text
VALID EVENT       → 1 output
INVALID EVENT     → 0 outputs
```

This is one reason `flatMap` is often more convenient than combining `filter()` and `map()`.

---

# 4. Generate multiple alerts from one event

Suppose you receive:

```java
SensorReading {
    sensorId = "S01",
    temperature = 105,
    pressure = 250
}
```

You want to generate alerts for **each violated condition**.

```java
stream.flatMap(
    (SensorReading reading, Collector<String> out) -> {

        if (reading.temperature > 100) {
            out.collect("HIGH_TEMPERATURE:" + reading.sensorId);
        }

        if (reading.pressure > 200) {
            out.collect("HIGH_PRESSURE:" + reading.sensorId);
        }
    }
);
```

One event can produce:

```text
SensorReading S01
       |
    flatMap
       |
       +----> HIGH_TEMPERATURE:S01
       |
       +----> HIGH_PRESSURE:S01
```

This is a very useful streaming pattern.

---

# 5. Extract multiple entities from an event

Imagine an application event:

```text
"Customer C101 purchased product P100
and product P200 using coupon SAVE20"
```

You might extract several events:

```text
CustomerEvent
ProductPurchase(P100)
ProductPurchase(P200)
CouponUsed(SAVE20)
```

`flatMap()` can transform the original event into these separate downstream events.

This is common in **event enrichment and event normalization**.

---

# 6. Handle multiple event types

Suppose Kafka contains different types of events:

```text
LOGIN,C101
PURCHASE,C101,P100,50
LOGOUT,C101
```

You could use `flatMap()` to convert them into a common event model.

```java
stream.flatMap(
    (String line, Collector<Event> out) -> {

        String[] fields = line.split(",");

        switch (fields[0]) {

            case "LOGIN":
                out.collect(new LoginEvent(fields[1]));
                break;

            case "PURCHASE":
                out.collect(
                    new PurchaseEvent(
                        fields[1],
                        fields[2],
                        Double.parseDouble(fields[3])
                    )
                );
                break;

            case "LOGOUT":
                out.collect(new LogoutEvent(fields[1]));
                break;
        }
    }
);
```

Here `flatMap()` is essentially acting as a **streaming parser/router**.

---

# 7. Split one event into different downstream events

Consider an IoT device event:

```java
DeviceReading {
    deviceId = "D1",
    temperature = 105,
    battery = 15,
    vibration = 80
}
```

You could produce several normalized events:

```text
TemperatureEvent(D1, 105)
BatteryEvent(D1, 15)
VibrationEvent(D1, 80)
```

```java
stream.flatMap(
    (DeviceReading r, Collector<DeviceEvent> out) -> {

        out.collect(
            new TemperatureEvent(r.deviceId, r.temperature)
        );

        out.collect(
            new BatteryEvent(r.deviceId, r.battery)
        );

        out.collect(
            new VibrationEvent(r.deviceId, r.vibration)
        );
    }
);
```

So:

```text
                 DeviceReading
                       |
                    flatMap
                       |
          +------------+------------+
          |            |            |
      Temperature    Battery    Vibration
        Event         Event        Event
```

---

# 8. Create multiple events from a time-series event

This becomes interesting in streaming analytics.

Suppose you receive:

```text
TemperatureReading
timestamp = 10:00
temperature = 72
```

Your application might generate:

```text
TemperatureReading
       |
    flatMap
       |
       +----> TemperatureMeasurement
       +----> TemperatureMetric
       +----> AuditEvent
```

For example:

```java
stream.flatMap(
    (TemperatureReading r, Collector<Event> out) -> {

        out.collect(new TemperatureMeasurement(r));

        if (r.temperature > 70) {
            out.collect(new TemperatureAlert(r));
        }

        out.collect(new AuditEvent(r));
    }
);
```

Notice something important:

**The same incoming event can generate different kinds of downstream events depending on its contents.**

---

# 9. Extract words from a continuous stream

This is particularly useful for illustrating why `flatMap` makes sense in streaming.

Imagine a Kafka topic continuously receives:

```text
"Flink is fast"
"Flink supports state"
"Flink supports event time"
```

Your stream is:

```text
       Kafka
         |
         v
"Flink is fast"
"Flink supports state"
"Flink supports event time"
         |
      flatMap
         |
         v
Flink
is
fast
Flink
supports
state
Flink
supports
event
time
```

You can then:

```java
words
    .map(word -> word.toLowerCase())
    .keyBy(word -> word)
    .sum("count");
```

This eventually gives you a streaming word count.

---

# 10. The most important distinction: `map` vs `flatMap`

Suppose you have:

```java
DataStream<String> input;
```

### `map`

```java
input.map(line -> line.toUpperCase());
```

Conceptually:

```text
Input                 Output

"hello"       --->    "HELLO"
"world"       --->    "WORLD"
```

**One → one**

---

### `filter`

```java
input.filter(line -> line.length() > 5);
```

```text
"hello"       --->    nothing
"streaming"   --->    "STREAMING"
```

**One → zero/one**

---

### `flatMap`

```java
input.flatMap(
    (String line, Collector<String> out) -> {
        for (String word : line.split(" ")) {
            out.collect(word);
        }
    }
);
```

```text
"hello world flink"
          |
        flatMap
          |
     +----+----+------+
     |    |    |      |
   hello world flink
```

**One → zero/one/many**

---

## A useful way to remember it

Think of `flatMap` as:

> **"Take this one event, look inside it, and emit however many events make sense."**

For example:

```text
               INPUT EVENT
                    |
                    v
                 flatMap
                    |
       +------------+-------------+
       |            |             |
       v            v             v
   output #1    output #2     output #3
```

Or potentially:

```text
INPUT EVENT
    |
    v
 flatMap
    |
    +----> nothing
```

That's why `flatMap` is particularly valuable in **stream normalization, parsing, event extraction, alert generation, and transforming nested/batched messages into individual events**.
