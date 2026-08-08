"""
Task 4 starter — Library analytics over JDBC (MySQL)

Run this with:
  docker compose exec spark /opt/spark/bin/spark-submit \
    --driver-class-path /opt/spark/jars-extra/mysql-connector-j.jar \
    --jars /opt/spark/jars-extra/mysql-connector-j.jar \
    /opt/spark/scripts/library_analytics.py

This script uses SparkSession.read.jdbc(...) instead of the
`CREATE TEMPORARY VIEW ... USING org.apache.spark.sql.jdbc` syntax you used
in spark-sql for Task 3. Same JDBC source, different API — that contrast is
intentional.
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("library-analytics")
    .master("local[*]")
    .getOrCreate()
)

JDBC_URL = "jdbc:mysql://mysql:3306/library"
JDBC_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "com.mysql.cj.jdbc.Driver",
}


def load_table(table_name: str):
    return spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPS)


authors = load_table("authors")
books = load_table("books")
members = load_table("members")
loans = load_table("loans")
fines = load_table("fines")

for name, df in [
    ("authors", authors),
    ("books", books),
    ("members", members),
    ("loans", loans),
    ("fines", fines),
]:
    df.createOrReplaceTempView(name)
    print(f"Registered temp view '{name}' with {df.count()} rows")


# ---------------------------------------------------------------------------
# WORKED EXAMPLE — fines collected by month (status = 'paid')
# Equivalent to the demo's "revenue by month" query, adapted to this schema.
# ---------------------------------------------------------------------------
print("\n=== Fines collected by month ===")
spark.sql(
    """
    SELECT
      date_format(paid_date, 'yyyy-MM') AS month,
      ROUND(SUM(amount), 2) AS total_collected,
      COUNT(*) AS fine_count
    FROM fines
    WHERE status = 'PAID'
    GROUP BY date_format(paid_date, 'yyyy-MM')
    ORDER BY month
    """
).show()


# ---------------------------------------------------------------------------
# TODO — write and run each of the following as its own spark.sql(...).show()
# block, directly below this comment. Keep them in order; you'll paste the
# output of each into ANSWERS.md.
# ---------------------------------------------------------------------------

# Query 2: Most active members — for each member, how many loans have
# they taken out in total, and how many are currently active or overdue?
# Order by total loans descending, top 10.
print("\n=== Most active members ===")
spark.sql(
    """
    SELECT
      m.name,
      m.country,
      COUNT(*) AS total_loans,
      SUM(CASE WHEN l.status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_loans,
      SUM(CASE WHEN l.status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_loans
    FROM loans l
    JOIN members m ON l.member_id = m.id
    GROUP BY m.id, m.name, m.country
    ORDER BY total_loans DESC
    LIMIT 10
    """
).show()

# Query 3: Fine status mix — count and total amount grouped by fines.status.
print("\n=== Fine status mix ===")
spark.sql(
    """
    SELECT
      status,
      COUNT(*) AS fine_count,
      ROUND(SUM(amount), 2) AS total_amount
    FROM fines
    GROUP BY status
    """
).show()

# Query 4: Loans by genre — join loans -> books, count loans and sum
# books.price per genre, ordered by loan count descending.
print("\n=== Loans by genre ===")
spark.sql(
    """
    SELECT
      b.genre,
      COUNT(*) AS loan_count,
      ROUND(SUM(b.price), 2) AS revenue_at_price
    FROM loans l
    JOIN books b ON l.book_id = b.id
    GROUP BY b.genre
    ORDER BY loan_count DESC
    """
).show()

# Query 5: Currently overdue report — list loan id, member name, book
# title, due_date, and days overdue (today - due_date) for every loan with
# status = 'overdue'.
# Uses a fixed AS_OF_DATE (2025-07-15, per data/seed.sql) instead of wall-clock
# CURDATE(), since the synthetic data is dated in 2025.
print("\n=== Currently overdue report ===")
spark.sql(
    """
    SELECT
      l.id AS loan_id,
      m.name AS member_name,
      b.title AS book_title,
      l.due_date,
      DATEDIFF(DATE'2025-07-15', l.due_date) AS days_overdue
    FROM loans l
    JOIN members m ON l.member_id = m.id
    JOIN books b ON l.book_id = b.id
    WHERE l.status = 'OVERDUE'
    ORDER BY days_overdue DESC
    """
).show(truncate=False)

# Query 6: Average loan duration (in days, using return_date - loan_date)
# by member country, for returned loans only. Order by average duration
# descending.
print("\n=== Average loan duration by member country ===")
spark.sql(
    """
    SELECT
      m.country,
      ROUND(AVG(DATEDIFF(l.return_date, l.loan_date)), 2) AS avg_loan_duration_days,
      COUNT(*) AS returned_loans
    FROM loans l
    JOIN members m ON l.member_id = m.id
    WHERE l.status = 'RETURNED'
    GROUP BY m.country
    ORDER BY avg_loan_duration_days DESC
    """
).show()

# Query 7: Most borrowed books — book title, author name, and total
# number of loans, ordered descending. Top 10.
print("\n=== Most borrowed books (top 10) ===")
spark.sql(
    """
    SELECT
      b.title,
      a.name AS author_name,
      COUNT(*) AS times_borrowed
    FROM loans l
    JOIN books b ON l.book_id = b.id
    JOIN authors a ON b.author_id = a.id
    GROUP BY b.id, b.title, a.name
    ORDER BY times_borrowed DESC
    LIMIT 10
    """
).show(truncate=False)

# Query 8: Monthly loan volume by status — count of loans grouped by
# loan month and status, ordered by month then status.
print("\n=== Monthly loan volume by status ===")
spark.sql(
    """
    SELECT
      date_format(l.loan_date, 'yyyy-MM') AS month,
      l.status,
      COUNT(*) AS loan_count
    FROM loans l
    GROUP BY date_format(l.loan_date, 'yyyy-MM'), l.status
    ORDER BY month, status
    """
).show()


spark.stop()
