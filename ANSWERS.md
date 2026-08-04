# ANSWERS.md — Spark 101 Take-Home Submission

## Task 1 — Docker

```
$ docker compose exec mysql mysql -u spark -pspark library -e "SHOW TABLES;"
+-------------------+
| Tables_in_library |
+-------------------+
| authors           |
| books             |
| fines             |
| loans             |
| members           |
+-------------------+
```

## Task 2 — Spark SQL over JDBC

```
$ docker compose exec spark /opt/spark/bin/spark-sql \
    --driver-class-path /opt/spark/jars-extra/mysql-connector-j.jar \
    --conf spark.executor.extraClassPath=/opt/spark/jars-extra/mysql-connector-j.jar \
    --conf spark.driver.extraClassPath=/opt/spark/jars-extra/mysql-connector-j.jar \
    -f /opt/spark/scripts/task2_views.sql
...
authors
books
fines
loans
members
Time taken: 0.446 seconds, Fetched 5 row(s)
10
Time taken: 0.517 seconds, Fetched 1 row(s)
20
Time taken: 0.099 seconds, Fetched 1 row(s)
```

## Task 3 — SparkSession vs SparkContext

```
$ docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/scripts/sparkcontext_and_session.py
SparkSession: <pyspark.sql.session.SparkSession object at 0xffffa10337f0>
SparkContext: <SparkContext master=local[*] appName=sparkcontext-and-session-demo>
Spark version: 3.5.1
App name: sparkcontext-and-session-demo
Master: local[*]
Default parallelism: 15
RDD sum(1..10) = 55
+---+
|  n|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
| 10|
+---+
```

**Reflection questions:**

1. Why does every SparkSession need a SparkContext underneath it?

   SparkSession is a higher-level facade introduced in Spark 2.0 to unify what
   used to be separate entry points (SQLContext, HiveContext, StreamingContext).
   Underneath, it still needs a SparkContext because that's the object that
   actually talks to the cluster manager (or local threads, in `local[*]`
   mode), schedules tasks, and tracks RDDs/broadcast variables/accumulators.
   The DataFrame/SQL API is compiled down to RDD operations by Catalyst under
   the hood, so there's no way to avoid needing a SparkContext — SparkSession
   just hides it for the common case.

2. When would you reach for `spark.sparkContext` directly instead of the DataFrame API?

   When I need raw RDD transformations on non-tabular data, custom
   partitioning control, accumulators for custom counters, or broadcast
   variables for a small lookup table shared across executors. For nearly
   all ETL/analytics work (like Task 4), the DataFrame API is the right
   choice since it's optimized and more concise.


## Task 4 — Library analytics

### 1. Fines collected by month
```
```

### 2. Most active members
```
```

### 3. Fine status mix
```
```

### 4. Loans by genre
```
```

### 5. Currently overdue report
```
```

### 6. Average loan duration by member country
```
```

### 7. Most borrowed books (top 10)
```
```

### 8. Monthly loan volume by status
```
```

## Reflection

1. What was the hardest part of wiring the JDBC connection yourself?
2. What broke that "just worked" in the demo, and why?
3. What's the difference between `CREATE TEMPORARY VIEW ... USING org.apache.spark.sql.jdbc` (Task 2) and `spark.read.jdbc(...)` (Task 4)?
4. What would you do differently if you did this again?

## Final commit graph

```
(paste `git log --oneline --graph --all` output here)
```
