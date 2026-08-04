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
+-------+---------------+----------+
|  month|total_collected|fines_paid|
+-------+---------------+----------+
|2025-01|           2.50|         1|
|2025-02|           3.00|         1|
|2025-03|           5.00|         2|
|2025-04|           4.00|         1|
|2025-06|           2.00|         1|
+-------+---------------+----------+
```

### 2. Most active members
```
+------------+---------+-----------+------------+-------------+
|        name|  country|total_loans|active_loans|overdue_loans|
+------------+---------+-----------+------------+-------------+
| Alice Smith|      USA|          3|           0|            0|
| Frank Brown|      USA|          3|           0|            0|
| Carol White|   Canada|          3|           0|            0|
|   Eva Green|    India|          3|           0|            0|
|    Ivy Chen|    India|          3|           1|            1|
|   David Lee|Australia|          3|           0|            1|
|Henry Wilson|   Canada|          3|           1|            0|
|   Grace Kim|       UK|          3|           1|            0|
|  Jack Davis|      USA|          3|           1|            0|
|   Bob Jones|       UK|          3|           0|            1|
+------------+---------+-----------+------------+-------------+
```

### 3. Fine status mix
```
+------+----------+------------+
|status|fine_count|total_amount|
+------+----------+------------+
|UNPAID|         8|      106.50|
|PAID  |         6|       16.50|
+------+----------+------------+
```

### 4. Loans by genre
```
+-----------+----------+----------------+
|      genre|loan_count|revenue_at_price|
+-----------+----------+----------------+
|    Fiction|        20|          277.88|
|    Fantasy|         6|          105.94|
|     Sci-Fi|         6|           88.96|
|    Mystery|         4|           44.98|
|Non-Fiction|         4|           77.96|
+-----------+----------+----------------+
```

### 5. Currently overdue report
```
(days_overdue computed against fixed AS_OF_DATE = 2025-07-15)
+-------+-----------+----------------------------------------+----------+------------+
|loan_id|member_name|book_title                              |due_date  |days_overdue|
+-------+-----------+----------------------------------------+----------+------------+
|24     |Ivy Chen   |Americanah                              |2025-04-26|80          |
|28     |Mia Hall   |I, Robot                                |2025-05-22|54          |
|32     |Bob Jones  |Harry Potter and the Chamber of Secrets |2025-06-17|28          |
|34     |David Lee  |Kafka on the Shore                      |2025-06-22|23          |
+-------+-----------+----------------------------------------+----------+------------+
```

### 6. Average loan duration by member country
```
+---------+----------------------+--------------+
|  country|avg_loan_duration_days|returned_loans|
+---------+----------------------+--------------+
|   Canada|                 16.86|             7|
|Australia|                  16.5|             4|
|       UK|                  14.5|             6|
|      USA|                  14.4|            10|
|    India|                  12.8|             5|
+---------+----------------------+--------------+
```

### 7. Most borrowed books (top 10)
```
+---+----------------------------+--------------+
|id |title                       |times_borrowed|
+---+----------------------------+--------------+
|1  |1984                        |2             |
|2  |Animal Farm                 |2             |
|3  |Foundation                  |2             |
|4  |I, Robot                    |2             |
|5  |Murder on the Orient Express|2             |
|6  |And Then There Were None    |2             |
|7  |Americanah                  |2             |
|8  |Half of a Yellow Sun        |2             |
|9  |Norwegian Wood              |2             |
|10 |Kafka on the Shore          |2             |
+---+----------------------------+--------------+
```

### 8. Monthly loan volume by status
```
+-------+--------+----------+
|  month|  status|loan_count|
+-------+--------+----------+
|2025-01|RETURNED|         6|
|2025-02|RETURNED|         6|
|2025-03|RETURNED|         6|
|2025-04|OVERDUE |         1|
|2025-04|RETURNED|         5|
|2025-05|OVERDUE |         1|
|2025-05|RETURNED|         5|
|2025-06|OVERDUE |         2|
|2025-06|RETURNED|         4|
|2025-07|ACTIVE  |         4|
+-------+--------+----------+
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
