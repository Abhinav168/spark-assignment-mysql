"""
Task 3 — SparkSession vs SparkContext.

Goal: understand that SparkSession is the modern entry point (DataFrames/SQL),
while SparkContext (`spark.sparkContext`) is the lower-level handle that
RDDs, accumulators, and broadcast variables actually run on.
"""

from pyspark.sql import SparkSession

# TODO 1 (code): create a SparkSession named "sparkcontext-and-session-demo",
# running locally with all available cores.
spark = (
    SparkSession.builder.appName("sparkcontext-and-session-demo")
    .master("local[*]")
    .getOrCreate()
)

# Every SparkSession wraps a SparkContext.
sc = spark.sparkContext
print(f"SparkSession: {spark}")
print(f"SparkContext: {sc}")
print(f"Spark version: {sc.version}")
print(f"App name: {sc.appName}")
print(f"Master: {sc.master}")
print(f"Default parallelism: {sc.defaultParallelism}")

# TODO 2 (code): use the SparkContext directly to build an RDD from a Python
# list of numbers 1..10, then compute and print its sum using RDD operations
# (not a DataFrame).
numbers_rdd = sc.parallelize(range(1, 11))
total = numbers_rdd.sum()
print(f"RDD sum(1..10) = {total}")

# Also show the same numbers as a DataFrame, built via SparkSession, to
# contrast the two APIs operating on the same underlying SparkContext.
df = spark.createDataFrame([(n,) for n in range(1, 11)], ["n"])
df.show()

# TODO 3 (reflection, in your own words): why does every SparkSession need a
# SparkContext underneath it, and when (if ever) would you reach for
# `spark.sparkContext` directly instead of just using the DataFrame API?
#
# Reflection: SparkSession is a higher-level wrapper that unifies what used
# to be separate contexts (SQLContext, HiveContext, StreamingContext) on top
# of a single SparkContext, which is the actual connection to the cluster
# (or local executors) that schedules tasks, tracks RDDs, and manages
# accumulators/broadcast variables. The DataFrame/SQL API you get from
# SparkSession is built on RDDs under the hood and is optimized by Catalyst/
# Tungsten, so for almost all day-to-day ETL and analytics work you should
# stay on the DataFrame API. You'd reach for `spark.sparkContext` directly
# when you need something the DataFrame API doesn't expose: raw RDD
# transformations on unstructured/non-tabular data, custom partitioning
# logic, accumulators for custom metrics, or broadcast variables for small
# lookup data shared across executors.

spark.stop()
