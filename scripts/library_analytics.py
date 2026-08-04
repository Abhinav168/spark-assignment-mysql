"""
Task 4 — Library analytics using SparkSession.read.jdbc.

Loads all five library tables over JDBC and runs a set of analytics
queries against them using the DataFrame API.
"""

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("library-analytics")
    .master("local[*]")
    .getOrCreate()
)

jdbc_url = "jdbc:mysql://mysql:3306/library"
conn_props = {
    "user": "spark",
    "password": "spark",
    "driver": "com.mysql.cj.jdbc.Driver",
}

authors = spark.read.jdbc(jdbc_url, "authors", properties=conn_props)
books = spark.read.jdbc(jdbc_url, "books", properties=conn_props)
members = spark.read.jdbc(jdbc_url, "members", properties=conn_props)
loans = spark.read.jdbc(jdbc_url, "loans", properties=conn_props)
fines = spark.read.jdbc(jdbc_url, "fines", properties=conn_props)

for df, name in [
    (authors, "authors"),
    (books, "books"),
    (members, "members"),
    (loans, "loans"),
    (fines, "fines"),
]:
    df.createOrReplaceTempView(name)

# Fixed reference date used for "currently overdue"-style analytics, since
# this dataset's timeline (Jan-Jul 2025) doesn't track wall-clock "today".
AS_OF_DATE = "2025-07-15"

# 1. Fines collected by month (worked example)
print("=== 1. Fines collected by month ===")
fines_by_month = (
    fines.filter(F.col("status") == "PAID")
    .withColumn("month", F.date_format("paid_date", "yyyy-MM"))
    .groupBy("month")
    .agg(
        F.sum("amount").alias("total_collected"),
        F.count("*").alias("fines_paid"),
    )
    .orderBy("month")
)
fines_by_month.show()

# 2. Most active members (total loans, split out active/overdue)
print("=== 2. Most active members ===")
member_loan_counts = loans.groupBy("member_id").agg(
    F.count("*").alias("total_loans"),
    F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_loans"),
    F.sum(F.when(F.col("status") == "OVERDUE", 1).otherwise(0)).alias("overdue_loans"),
)
member_activity = (
    member_loan_counts.join(members, member_loan_counts.member_id == members.id)
    .select(members.name, members.country, "total_loans", "active_loans", "overdue_loans")
    .orderBy(F.desc("total_loans"))
)
member_activity.show(10)

# 3. Fine status mix (count + total amount by status)
print("=== 3. Fine status mix ===")
fine_status_mix = fines.groupBy("status").agg(
    F.count("*").alias("fine_count"),
    F.sum("amount").alias("total_amount"),
)
fine_status_mix.show()

# 4. Loans by genre (popularity + revenue-at-price)
print("=== 4. Loans by genre ===")
loans_by_genre = (
    loans.join(books, loans.book_id == books.id)
    .groupBy("genre")
    .agg(
        F.count("*").alias("loan_count"),
        F.sum("price").alias("revenue_at_price"),
    )
    .orderBy(F.desc("loan_count"))
)
loans_by_genre.show()

# 5. Currently overdue report (loan id, member, title, days overdue)
print("=== 5. Currently overdue report ===")
overdue_report = (
    loans.filter(F.col("status") == "OVERDUE")
    .join(members, loans.member_id == members.id)
    .join(books, loans.book_id == books.id)
    .select(
        loans.id.alias("loan_id"),
        members.name.alias("member_name"),
        books.title.alias("book_title"),
        loans.due_date,
        F.datediff(F.lit(AS_OF_DATE), loans.due_date).alias("days_overdue"),
    )
    .orderBy(F.desc("days_overdue"))
)
overdue_report.show(truncate=False)

# 6. Average loan duration by member country (returned loans only)
print("=== 6. Average loan duration by member country ===")
avg_duration_by_country = (
    loans.filter(F.col("status") == "RETURNED")
    .withColumn("duration_days", F.datediff("return_date", "loan_date"))
    .join(members, loans.member_id == members.id)
    .groupBy(members.country)
    .agg(
        F.round(F.avg("duration_days"), 2).alias("avg_loan_duration_days"),
        F.count("*").alias("returned_loans"),
    )
    .orderBy(F.desc("avg_loan_duration_days"))
)
avg_duration_by_country.show()

# 7. Most borrowed books (top 10)
print("=== 7. Most borrowed books (top 10) ===")
most_borrowed = (
    loans.join(books, loans.book_id == books.id)
    .groupBy(books.id, books.title)
    .agg(F.count("*").alias("times_borrowed"))
    .orderBy(F.desc("times_borrowed"))
)
most_borrowed.show(10, truncate=False)

# 8. Monthly loan volume by status
print("=== 8. Monthly loan volume by status ===")
monthly_volume = (
    loans.withColumn("month", F.date_format("loan_date", "yyyy-MM"))
    .groupBy("month", "status")
    .agg(F.count("*").alias("loan_count"))
    .orderBy("month", "status")
)
monthly_volume.show(50)

spark.stop()
