# JoinFunction 

`JoinFunction` is a simple interface you implement when you want to **combine two pieces of data that match on a key**. It has one method:

```java
OUT join(IN1 first, IN2 second)
```

Flink calls this method **once for every matching pair**.  You receive one element from the first stream/dataset and one from the second, and you return the combined result. By default it behaves like an **inner join** (only matching pairs are kept).

---

### How you typically use it

**In the DataStream API (windowed join):**
```java
stream1.join(stream2)
    .where(keySelector1)
    .equalTo(keySelector2)
    .window(...)
    .apply(new MyJoinFunction());
```

---

### Real-World Examples

#### 1. E-commerce: Enrich an Order with Customer Details

You have:
- A stream of **Orders** (orderId, customerId, amount)
- A stream of **Customers** (customerId, name, city)

You want every order to also contain the customer’s name and city.

```java
orders.join(customers)
    .where(order -> order.customerId)
    .equalTo(customer -> customer.customerId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .apply(new JoinFunction<Order, Customer, EnrichedOrder>() {
        @Override
        public EnrichedOrder join(Order order, Customer customer) {
            return new EnrichedOrder(
                order.orderId,
                order.amount,
                customer.name,
                customer.city
            );
        }
    });
```

**Result:** Only orders that have a matching customer in the same window appear, fully enriched.

---

#### 2. Grades + Salaries (Classic Flink Example)

You have two streams of people:
- Grades: (name, grade)
- Salaries: (name, salary)

You want to produce (name, grade, salary) for people who appear in both.

```java
grades.join(salaries)
    .where(g -> g.f0)          // name
    .equalTo(s -> s.f0)        // name
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
    .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
        @Override
        public Tuple3<String, Integer, Integer> join(
                Tuple2<String, Integer> grade,
                Tuple2<String, Integer> salary) {
            return Tuple3.of(grade.f0, grade.f1, salary.f1);
        }
    });
```

---

#### 3. Currency Conversion for Orders

You have:
- Orders in different currencies (orderId, amount, currency)
- Live exchange rates (currency, rate)

You want the order amount converted to USD.

```java
orders.join(rates)
    .where(order -> order.currency)
    .equalTo(rate -> rate.currency)
    .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
    .apply(new JoinFunction<Order, ExchangeRate, ConvertedOrder>() {
        @Override
        public ConvertedOrder join(Order order, ExchangeRate rate) {
            double usdAmount = order.amount * rate.rate;
            return new ConvertedOrder(order.orderId, usdAmount);
        }
    });
```

---

#### 4. Student Information + Exam Score

You have student profiles and exam results. You want a combined record for every student who took the exam.

```java
studentInfo.join(examScores)
    .where(s -> s.studentId)
    .equalTo(e -> e.studentId)
    .window(...)
    .apply((student, score) -> 
        new EnrichedResult(student.id, student.name, score.points)
    );
```

---

### Key Points to Remember
- Flink recommends the Table/SQL API for joins, but `JoinFunction` is still the way to write the custom combination logic when using the DataStream window-join API.
- `JoinFunction` is called **once per matching pair**. It is an **inner join** by default (non-matching records are dropped).
- Flink provides `CoGroupFunction` for outer joins (non-matching records are retained). 
