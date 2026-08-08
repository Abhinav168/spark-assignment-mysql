"""
Task 3 starter — SparkSession vs SparkContext

Run this with:
  docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/scripts/sparkcontext_and_session.py

Goal: understand the relationship between SparkSession and SparkContext before
you touch any real data. Fill in every TODO. Do not delete the print()
statements — they are your proof-of-work for this task.
"""

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# 1. SparkSession — the modern, unified entry point (DataFrames, SQL, streaming)
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("spark-101-assignment")
    .master("local[*]")
    .config("spark.jars", "/opt/spark/jars-extra/mysql-connector-j.jar")
    .getOrCreate()
)

print(f"Spark version: {spark.version}")
print(f"App name (via SparkSession): {spark.conf.get('spark.app.name')}")

# ---------------------------------------------------------------------------
# 2. SparkContext — the original, lower-level entry point.
#    Every SparkSession wraps exactly one SparkContext. RDD operations,
#    accumulators, and broadcast variables all go through it.
# ---------------------------------------------------------------------------
sc = spark.sparkContext

print(f"SparkContext master: {sc.master}")
print(f"SparkContext app name: {sc.appName}")
print(f"Default parallelism: {sc.defaultParallelism}")

# TODO 1: Use sc.parallelize(...) to create an RDD from a Python list of the
# numbers 1-20, then use .filter() and .map() to compute the squares of the
# even numbers. Collect and print the result.
even_squares = sc.parallelize(range(1, 21)).filter(lambda n: n % 2 == 0).map(lambda n: n * n).collect()
print(f"Even squares: {even_squares}")


# TODO 2: Create a broadcast variable containing a small dict, e.g.
# {"pending": "needs follow-up", "overdue": "urgent", "returned": "closed"}.
# Use it inside an RDD .map() to label a list of loan statuses, then collect
# and print the labelled result.
status_lookup = sc.broadcast({"pending": "needs follow-up", "overdue": "urgent", "returned": "closed"})
labelled = sc.parallelize(["pending", "overdue", "returned"]).map(lambda s: (s, status_lookup.value.get(s))).collect()
print(f"Labelled statuses: {labelled}")


# TODO 3: In 2-3 lines of code comments (not prose in the README — actual
# comments right here), explain in your own words:
#   a) What does spark.stop() actually shut down — the session, the context,
#      or both?
#   b) Could you create a second SparkSession in the same JVM process while
#      this one is still running? What would spark.sparkContext point to if
#      you did?
#
# a) spark.stop() shuts down both: it stops the underlying SparkContext (which
#    tears down executors/tasks and releases cluster resources), and the
#    SparkSession that wraps it becomes unusable since it has no context left.
# b) Yes — SparkSession.builder.getOrCreate() would just hand back the same
#    underlying SparkContext (only one SparkContext is allowed per JVM), so
#    spark.sparkContext on the "second" session would point to the exact same
#    SparkContext object as the first, not a new one.

spark.stop()
