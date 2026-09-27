# keyBy Operator
`keyBy()` is one of the **most important concepts in Flink**, because it determines **which events are grouped together and therefore where Flink keeps their state**.

A simple mental model:

> **`keyBy()` says: "Events with the same key belong together."**

Once you `keyBy()`, Flink can maintain **separate state for each key**.

---

# 1. Sensor readings — the classic example

Suppose your stream contains:

```text
SensorReading
-------------------------
S01   72.1
S02   68.5
S01   73.2
S03   81.4
S02   69.1
S01   74.0
```

You want to calculate a running average **for each sensor**.

```java
DataStream<SensorReading> readings = ...;

readings
    .keyBy(reading -> reading.sensorId)
    .process(new RunningAverage());
```

The important part is:

```java
.keyBy(reading -> reading.sensorId)
```

Flink effectively creates logical groups:

```text
             Sensor readings
                    |
                  keyBy
                    |
       +------------+------------+
       |            |            |
       v            v            v
     S01          S02          S03
       |            |            |
       v            v            v
  state for S01  state S02   state S03
```

So when an `S01` event arrives, it accesses **S01's state**, not S02's.

This is exactly the idea behind your earlier `RunningAverage` example.

---

# 2. Customer transactions

Suppose your stream contains:

```text
Customer   Transaction
-----------------------
C101       $50
C102       $20
C101       $30
C103       $100
C102       $40
C101       $25
```

You want to maintain:

> Total amount spent by each customer.

```java
transactions
    .keyBy(t -> t.customerId)
    .process(new CustomerSpendingProcessFunction());
```

Conceptually:

```text
                    transactions
                          |
                        keyBy
                          |
           +--------------+--------------+
           |              |              |
           v              v              v
         C101           C102           C103
           |              |              |
           v              v              v
       $50+$30+$25     $20+$40          $100
           |              |              |
           v              v              v
         $105            $60            $100
```

Each customer has **independent state**.

---

# 3. Detect suspicious activity per account

Imagine a banking stream:

```text
Account   Amount
----------------
A100      $500
A200      $50
A100      $900
A100      $700
A200      $40
```

You want to detect:

> More than $2,000 of transactions for the same account within some period.

You might write:

```java
transactions
    .keyBy(t -> t.accountId)
    .process(new FraudDetectionFunction());
```

Why `keyBy()`?

Because you don't want:

```text
A100 + A200
```

to be treated as one customer's activity.

You want:

```text
A100 → its own state
A200 → its own state
```

This is a very common pattern in real streaming systems.

---

# 4. Web clicks — count clicks per user

Suppose:

```text
User     Page
----------------
U1       /home
U2       /products
U1       /cart
U1       /checkout
U2       /home
```

You want:

> Number of clicks per user.

```java
clicks
    .keyBy(click -> click.userId)
    .process(...);
```

Flink creates logical partitions:

```text
             Click Stream
                  |
                keyBy
                  |
       +----------+----------+
       |          |          |
       v          v          v
      U1         U2         U3
       |          |
       v          v
   click count  click count
```

---

# 5. Key by device

Consider an IoT application:

```text
Device    Temperature
---------------------
D1        70
D2        72
D1        71
D3        68
D2        73
D1        75
```

You might want:

> Detect when an individual device's temperature exceeds 74°F.

```java
readings
    .keyBy(r -> r.deviceId)
    .process(new TemperatureAlertFunction());
```

Now the state is effectively:

```text
D1 → temperature history
D2 → temperature history
D3 → temperature history
```

The key is what makes **per-device state** possible.

---

# 6. Key by composite key

This is where `keyBy()` becomes especially interesting.

Suppose your events contain:

```text
Country   Product   Sales
-------------------------
US        A         100
US        B         200
India     A         150
US        A         50
India     A         100
```

You want:

> Sales for each **country + product combination**.

You can use:

```java
sales
    .keyBy(s -> Tuple2.of(s.country, s.product))
```

Now your keys are:

```text
(US, A)
(US, B)
(India, A)
```

Conceptually:

```text
                    Sales
                      |
                    keyBy
                      |
        +-------------+-------------+
        |             |             |
        v             v             v
     (US,A)        (US,B)       (India,A)
        |             |             |
        v             v             v
       $150          $200           $250
```

This is extremely useful for multidimensional aggregations.

---

# 7. Key by stock symbol

Imagine a stock market stream:

```text
Symbol    Price
----------------
AAPL      220
MSFT      510
AAPL      221
GOOG      250
MSFT      512
AAPL      218
```

You want to maintain a moving calculation for **each stock**.

```java
prices
    .keyBy(p -> p.symbol)
    .process(new StockPriceFunction());
```

Flink maintains independent state:

```text
AAPL → state
MSFT → state
GOOG → state
```

So an AAPL event never modifies MSFT's state.

---

# 8. Key by order ID

Suppose an e-commerce system produces:

```text
OrderId   Event
---------------------------
O100      ORDER_CREATED
O200      ORDER_CREATED
O100      PAYMENT_RECEIVED
O100      SHIPPED
O200      PAYMENT_RECEIVED
```

You want to reconstruct the lifecycle of each order.

```java
events
    .keyBy(e -> e.orderId)
    .process(new OrderLifecycleFunction());
```

Now:

```text
              Events
                 |
               keyBy
                 |
        +--------+--------+
        |                 |
        v                 v
       O100              O200
        |                 |
        v                 v
 CREATED               CREATED
 PAYMENT               PAYMENT
 SHIPPED
```

The `O100` state might look like:

```text
orderId = O100
created = true
paid = true
shipped = true
```

---

# 9. Key by IP address

Suppose you are monitoring web traffic:

```text
IP Address       Request
-------------------------
10.1.1.10        /login
10.1.1.20        /home
10.1.1.10        /login
10.1.1.10        /login
10.1.1.20        /products
```

You could:

```java
requests
    .keyBy(r -> r.ipAddress)
```

Then maintain:

```text
10.1.1.10 → login attempts
10.1.1.20 → login attempts
```

This can be used for rate limiting or detecting repeated login attempts.

---

# 10. Key by employee

Consider an HR/time-tracking stream:

```text
Employee    Event
-----------------------
E101        CLOCK_IN
E102        CLOCK_IN
E101        BREAK_START
E101        BREAK_END
E102        CLOCK_OUT
```

You might want to maintain the current status of each employee.

```java
events
    .keyBy(e -> e.employeeId)
    .process(new EmployeeStatusFunction());
```

State:

```text
E101 → WORKING
E102 → OFF_WORK
```

Again, each key has independent state.

---

# 11. `keyBy()` + window

This is probably one of the most important combinations to understand.

Suppose:

```text
SensorReading
```

and you want:

> Average temperature for each sensor every 10 seconds.

```java
readings
    .keyBy(r -> r.sensorId)
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
    .aggregate(new AverageTemperature());
```

The conceptual picture is:

```text
                  readings
                     |
                   keyBy
                     |
          +----------+----------+
          |          |          |
          v          v          v
         S01        S02        S03
          |          |          |
       10-sec      10-sec      10-sec
       windows     windows     windows
          |          |          |
          v          v          v
       average    average    average
```

So `keyBy()` determines **whose events belong in the same window**.

---

# 12. What actually happens inside Flink?

This is the part that is often confusing.

Suppose:

```java
stream.keyBy(event -> event.customerId)
```

You might imagine:

```text
Customer C1 → TaskManager 1
Customer C2 → TaskManager 2
Customer C3 → TaskManager 3
```

But that's not necessarily how it works.

Flink hashes the key:

```text
customerId
     |
     v
 hash(key)
     |
     v
 key-group
     |
     v
 parallel task
```

For example:

```text
C101 ──┐
       ├──> hash ──> Key Group 3 ──> Task 1
C102 ──┤
       └──> hash ──> Key Group 7 ──> Task 2

C103 ─────> hash ──> Key Group 3 ──> Task 1
```

Therefore:

> **All events with the same key are routed to the same parallel task.**

That is what allows Flink to maintain consistent keyed state.

---

# 13. Why `keyBy()` is essential for state

Consider:

```java
stream
    .keyBy(r -> r.sensorId)
    .process(new RunningAverage());
```

Inside `RunningAverage`:

```java
private transient AggregatingState<Double, Double> average;
```

The important concept is:

```text
                    Stream
                       |
                    keyBy
                       |
        +--------------+--------------+
        |              |              |
        v              v              v
       S01            S02            S03
        |              |              |
        v              v              v
   State[S01]      State[S02]      State[S03]
```

When S01 arrives:

```text
S01 event
   |
   v
State[S01]
```

When S02 arrives:

```text
S02 event
   |
   v
State[S02]
```

The same `AggregatingState` definition is used, but **Flink maintains a separate logical state value for every key**.

This is the fundamental idea behind **keyed state**.

---

# 14. A very important distinction

Compare these:

```java
stream.map(...)
```

```text
Transform every event
```

```java
stream.filter(...)
```

```text
Decide whether to keep an event
```

```java
stream.flatMap(...)
```

```text
Turn one event into zero/many events
```

```java
stream.keyBy(...)
```

```text
Decide which events belong together
```

That last one is the key idea.

### Think of `keyBy()` as creating "buckets"

```text
                    Incoming events
                          |
                        keyBy
                          |
       +------------------+------------------+
       |                  |                  |
       v                  v                  v
    Bucket A           Bucket B           Bucket C
     key=A              key=B              key=C
       |                  |                  |
       v                  v                  v
    state A             state B             state C
```

And this leads directly to the next major Flink concept:

> **`keyBy()` → keyed stream → keyed state → stateful processing**

For a junior Java developer learning Flink, I'd teach these three together:

```text
                 keyBy()
                    |
                    v
              KeyedStream
                    |
          +---------+---------+
          |                   |
          v                   v
      window()           process()
          |                   |
          v                   v
   per-key windows      per-key state
```

That mental model makes `KeyedProcessFunction`, `ValueState`, `MapState`, `AggregatingState`, timers, and keyed windows much easier to understand.
