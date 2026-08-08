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
Spark version: 3.5.1
App name (via SparkSession): spark-101-assignment
SparkContext master: local[*]
SparkContext app name: spark-101-assignment
Default parallelism: 15
Even squares: [4, 16, 36, 64, 100, 144, 196, 256, 324, 400]
Labelled statuses: [('pending', 'needs follow-up'), ('overdue', 'urgent'), ('returned', 'closed')]
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

   When I need raw RDD transformations on non-tabular data (like the even-squares
   filter/map or the broadcast-variable status lookup above), custom
   partitioning control, accumulators for custom counters, or broadcast
   variables for a small lookup table shared across executors. For nearly
   all ETL/analytics work (like Task 4), the DataFrame API is the right
   choice since it's optimized and more concise.

3. `spark.stop()` and multiple sessions (from the in-code TODO 3 comments):

   `spark.stop()` shuts down both — it stops the underlying `SparkContext`
   (tearing down executors and releasing cluster resources), which leaves the
   wrapping `SparkSession` unusable since it has nothing left to run on. A
   second `SparkSession.builder.getOrCreate()` call in the same JVM would not
   create a new `SparkContext` — only one is allowed per JVM — so
   `spark.sparkContext` on that "second" session would just point to the same
   underlying `SparkContext` as the first.


## Task 4 — Library analytics

```
$ docker compose exec spark /opt/spark/bin/spark-submit \
    --driver-class-path /opt/spark/jars-extra/mysql-connector-j.jar \
    --jars /opt/spark/jars-extra/mysql-connector-j.jar \
    /opt/spark/scripts/library_analytics.py
Registered temp view 'authors' with 10 rows
Registered temp view 'books' with 20 rows
Registered temp view 'members' with 15 rows
Registered temp view 'loans' with 40 rows
Registered temp view 'fines' with 14 rows
```

> Note: the worked example as originally given referenced a `paid_at` column
> and lowercase `status = 'paid'`, but the actual seeded schema
> (`data/seed.sql`) uses `paid_date` and an uppercase `PAID`/`UNPAID` enum —
> fixed that mismatch before writing the remaining queries.

### 1. Fines collected by month
```
+-------+---------------+----------+
|  month|total_collected|fine_count|
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
|   Bob Jones|       UK|          3|           0|            1|
| Carol White|   Canada|          3|           0|            0|
|   David Lee|Australia|          3|           0|            1|
|   Eva Green|    India|          3|           0|            0|
| Frank Brown|      USA|          3|           0|            0|
|   Grace Kim|       UK|          3|           1|            0|
|Henry Wilson|   Canada|          3|           1|            0|
|    Ivy Chen|    India|          3|           1|            1|
|  Jack Davis|      USA|          3|           1|            0|
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
(days_overdue computed against fixed AS_OF_DATE = 2025-07-15, per data/seed.sql,
instead of wall-clock CURDATE() — the synthetic loan data is dated in 2025)
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
+-------------------------------------+----------------------+--------------+
|title                                |author_name           |times_borrowed|
+-------------------------------------+----------------------+--------------+
|Oryx and Crake                       |Margaret Atwood       |2             |
|1984                                 |George Orwell         |2             |
|Harry Potter and the Sorcerer's Stone|J.K. Rowling          |2             |
|Animal Farm                          |George Orwell         |2             |
|Foundation                           |Isaac Asimov          |2             |
|One Hundred Years of Solitude        |Gabriel Garcia Marquez|2             |
|Song of Solomon                      |Toni Morrison         |2             |
|Norwegian Wood                       |Haruki Murakami       |2             |
|Murder on the Orient Express         |Agatha Christie       |2             |
|Sapiens                              |Yuval Noah Harari     |2             |
+-------------------------------------+----------------------+--------------+
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

1. **What was the hardest part of wiring the JDBC connection yourself?**
   Getting the classpath right in three different places at once: the
   `spark-sql`/`spark-submit` `--driver-class-path`, `spark.driver.extraClassPath`,
   and `spark.executor.extraClassPath` all need to point at the same jar
   inside the container (`/opt/spark/jars-extra/mysql-connector-j.jar`), and
   the driver class name (`com.mysql.cj.jdbc.Driver`) has to match the jar's
   actual package — easy to typo and get an opaque `ClassNotFoundException`
   instead of a clear error.

2. **What broke that "just worked" in the demo, and why?**
   Two things: (a) the Spark container's hostname for MySQL is the Compose
   service name (`mysql`), not `localhost` — obvious once stated, easy to
   get wrong copying from a Postgres example with a different service name.
   (b) The synthetic loan/fine data is dated in 2025, but "today" when this
   was actually run was well past that — so any "currently overdue" query
   based on `CURDATE()`/wall-clock time would have misclassified every loan
   as overdue. I fixed this by storing `status` explicitly on the `loans`
   table and using a fixed `AS_OF_DATE` reference constant for day-count
   calculations, instead of comparing against real "now".

3. **Difference between `CREATE TEMPORARY VIEW ... USING org.apache.spark.sql.jdbc` (Task 2) and `spark.read.jdbc(...)` (Task 4)?**
   Both go through the same JDBC data source under the hood, but the first
   is SQL-shell syntax that registers a named temp view you can then query
   with plain SQL (`SELECT ... FROM authors`), while `spark.read.jdbc(...)`
   is the PySpark DataFrame API entry point — it returns a DataFrame you
   manipulate programmatically (and can optionally register as a temp view
   yourself via `createOrReplaceTempView`, which is what `library_analytics.py`
   does so both SQL and DataFrame styles are available).

4. **What would you do differently if you did this again?**
   Redirect `spark-submit` output straight to a log file from the start
   instead of relying on the terminal buffer — the driver/executor INFO
   logs are extremely verbose and buried the actual query output, which
   made it easy to lose the first few queries' results when the console
   buffer got truncated.

## Final commit graph

```
$ git log --oneline --graph --all
* 80c82ec (HEAD -> main, tag: v1.0, origin/main, origin/HEAD) Task 4: 7 library analytics queries
* 4f45459 Task 3: SparkSession vs SparkContext demo + reflection
* 3cd00e9 Add final commit graph to ANSWERS.md
* 028c1c2 Complete reflection section
*   d6c651e Merge pull request #4 from Abhinav168/task-4-library-analytics
|\
| * b2f5aab (origin/task-4-library-analytics) Task 4: 7 library analytics queries
|/
*   4d4f347 Merge pull request #3 from Abhinav168/task-3-spark-session-context
|\
| * 5eff93b (origin/task-3-spark-session-context) Task 3: SparkSession vs SparkContext demo + reflection
|/
*   f46f6f0 Merge pull request #2 from Abhinav168/task-2-spark-sql
|\
| * 03cdda5 (origin/task-2-spark-sql) Task 2: Spark SQL JDBC temp views over MySQL
|/
*   2e4489e Merge pull request #1 from Abhinav168/task-1-docker-setup
|\
| * fc54fdb (origin/task-1-docker-setup) Task 1: MySQL + Spark stack running via Docker Compose
|/
* d23bcf2 Initial assignment prompt
```

> Note: after the assignment's starter files were refreshed mid-submission,
> Task 3 and Task 4 were redone directly on top of `main` (commits `4f45459`
> and `80c82ec`) and merged straight in rather than via separate PR branches,
> since the branches from the original PRs (`task-3-spark-session-context`,
> `task-4-library-analytics`) had already been merged once. The `v1.0` tag was
> moved to `80c82ec` to mark the actual final state.
