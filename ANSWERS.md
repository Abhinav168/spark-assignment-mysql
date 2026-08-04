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
(paste SHOW TABLES / COUNT(*) output here)
```

## Task 3 — SparkSession vs SparkContext

```
(paste spark-submit console output here)
```

**Reflection questions:**

1. Why does every SparkSession need a SparkContext underneath it?
2. When would you reach for `spark.sparkContext` directly instead of the DataFrame API?

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
