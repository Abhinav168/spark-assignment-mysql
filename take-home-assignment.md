# Spark 101 — Take-Home Assignment: Library Analytics on MySQL

This is the hands-on companion to the in-class demo (ecommerce data on
Postgres). Same core skills — Docker, Spark SQL over JDBC, SparkSession /
SparkContext — but you're pointing Spark at a **different data source
(MySQL)** and a **different domain (a library system)**, and you're doing it
end-to-end with a proper Git branch/PR workflow.

Nobody is handing you a working `jars/mysql-connector-j.jar` or a finished
query — you're wiring the JDBC connection yourself and writing the queries
yourself. If something in the demo "just worked," expect to have to think
about *why* it worked before it works here.

## What you'll practice

- **Docker**: multi-container Compose stack (MySQL + Spark), healthchecks, volumes, container-to-container networking
- **Spark**: `spark-sql` shell over JDBC, `SparkSession`, `SparkContext`, RDDs, DataFrames, lazy evaluation
- **Git**: branching, incremental commits, opening a PR against `main`, resolving a merge, tagging a release

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Compose v2 (`docker compose`)
- A GitHub (or GitLab) account
- Run all commands from the project root unless told otherwise

## Project layout

```text
.
├── docker-compose.yml              # mysql + spark
├── data/seed.sql                   # library schema and sample rows
├── jars/                           # MySQL JDBC driver goes here (you download it)
├── scripts/
│   ├── sparkcontext_and_session.py # Task 3 starter (has TODOs)
│   └── library_analytics.py        # Task 4 starter (has TODOs)
├── ANSWERS.md                      # fill this in as you go — it's your submission
└── README.md
```

---

## Step 0 — Fork, clone, and set up your working branch

1. Fork this repository to your own GitHub account.
2. Clone your fork locally:

   ```bash
   git clone https://github.com/<your-username>/spark-101.git spark-101-assignment
   cd spark-101-assignment
   ```

3. Confirm `main` is your baseline and create your first working branch:

   ```bash
   git checkout main
   git checkout -b task-1-docker-setup
   ```

You will create one branch per task (listed below), commit as you complete
each one, push it, and open a PR into `main`. Don't do all five tasks on one
giant branch — the point is to practice the branch → PR → merge cycle
repeatedly.

---

## Task 1 — Docker: get MySQL + Spark running

Branch: `task-1-docker-setup`

### 1a. Download the MySQL JDBC driver (one-time)

Same trick as the demo — use a throwaway container so you don't need curl on
the host:

```bash
docker run --rm --user root -v "./jars:/jars" curlimages/curl:8.5.0 \
  -fsSL -o /jars/mysql-connector-j.jar \
  "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar"
```

Confirm it downloaded:

```bash
docker run --rm -v "./jars:/jars" busybox ls -la /jars/mysql-connector-j.jar
```

### 1b. Start the stack

```bash
docker compose up -d
```

Wait for MySQL to report healthy (`docker compose ps`), then list tables:

```bash
docker compose exec mysql mysql -u spark -pspark library -e "SHOW TABLES;"
```

You should see `authors`, `books`, `members`, `loans`, and `fines`.

> **Checkpoint:** paste this output into the "Task 1 — Docker" section of
> `ANSWERS.md`.

### 1c. Commit and open your first PR

```bash
git add jars/.gitkeep docker-compose.yml
git commit -m "Task 1: MySQL + Spark stack running via Docker Compose"
git push -u origin task-1-docker-setup
```

Open a PR: `task-1-docker-setup` → `main`. Merge it (use "Squash and merge" or
plain "Merge" — pick one and stay consistent). Then update local `main`:

```bash
git checkout main
git pull
```

> Do **not** commit `jars/mysql-connector-j.jar` itself — the `.gitignore`
> already excludes it. Only `jars/.gitkeep` should be tracked. If you're
> asked in review why, that's intentional: think about repo size and licensing
> before you answer.

---

## Task 2 — Spark SQL over JDBC (the shell, like the demo)

Branch: `task-2-spark-sql` (cut from `main` after Task 1 is merged)

```bash
git checkout main && git pull
git checkout -b task-2-spark-sql
```

Open a Spark SQL shell, but point the classpath at **your** driver:

```bash
docker compose exec spark /opt/spark/bin/spark-sql \
  --driver-class-path /opt/spark/jars-extra/mysql-connector-j.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars-extra/mysql-connector-j.jar \
  --conf spark.driver.extraClassPath=/opt/spark/jars-extra/mysql-connector-j.jar
```

Register temporary views for all five tables. The demo used
`org.apache.spark.sql.jdbc` against Postgres — you'll use the same format
provider, but the `url`, `driver`, and container hostname all need to change
for MySQL. Work out the correct `OPTIONS` block yourself; here's the shape
for one table to get you started:

```sql
CREATE TEMPORARY VIEW authors
USING org.apache.spark.sql.jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/library',
  driver 'com.mysql.cj.jdbc.Driver',
  dbtable 'authors',
  user 'spark',
  password 'spark'
);
```

Register the remaining four (`books`, `members`, `loans`, `fines`) yourself.

Run:

```sql
SHOW TABLES;
SELECT COUNT(*) AS authors FROM authors;
SELECT COUNT(*) AS books FROM books;
```

> **Checkpoint:** paste this output into `ANSWERS.md` under Task 2.

Exit with `exit;`, commit, push, open PR into `main`, merge, sync `main`
locally — same cycle as Task 1.

---

## Task 3 — SparkSession vs SparkContext

Branch: `task-3-spark-session-context`

Open `scripts/sparkcontext_and_session.py`. Complete all three `TODO`s (two
code, one code-comment reflection). This task deliberately does **not**
touch MySQL — it's about understanding the object model:

- `SparkSession` is the entry point you use for DataFrames and SQL.
- `SparkContext` (`spark.sparkContext`) is the lower-level handle every
  session wraps — it's what RDDs, accumulators, and broadcast variables
  actually run on.

Run it:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  /opt/spark/scripts/sparkcontext_and_session.py
```

> **Checkpoint:** paste the full console output into `ANSWERS.md`, and answer
> the two reflection questions there in your own words (not just the code
> comments).

Commit, push, PR, merge, sync.

---

## Task 4 — Library analytics (PySpark, `SparkSession.read.jdbc`)

Branch: `task-4-library-analytics`

Open `scripts/library_analytics.py`. It already loads all five tables via
`spark.read.jdbc(...)` (note: this is a different API from the
`CREATE TEMPORARY VIEW ... USING org.apache.spark.sql.jdbc` syntax you used
in Task 2 — same JDBC source, two different ways to reach it) and includes
one fully worked query. Write the remaining **7 queries** described in the
`TODO` comments:

1. ~~Fines collected by month~~ *(worked example — already done)*
2. Most active members (total loans, split out active/overdue)
3. Fine status mix (count + total amount by status)
4. Loans by genre (popularity + revenue-at-price)
5. Currently overdue report (loan id, member, title, days overdue)
6. Average loan duration by member country (returned loans only)
7. Most borrowed books (top 10)
8. Monthly loan volume by status

Run it:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --driver-class-path /opt/spark/jars-extra/mysql-connector-j.jar \
  --jars /opt/spark/jars-extra/mysql-connector-j.jar \
  /opt/spark/scripts/library_analytics.py
```

> **Checkpoint:** paste each query's `.show()` output into the corresponding
> section of `ANSWERS.md`.

Commit, push, PR, merge, sync.

---

## Task 5 — Wrap-up: tag a release

Branch: not needed — do this on `main` after Task 4 is merged.

1. Finish filling in the **Reflection** section of `ANSWERS.md` (4 questions
   — these are about what you actually experienced, not the happy path).
2. Commit and push directly to `main`:

   ```bash
   git add ANSWERS.md
   git commit -m "Complete reflection section"
   git push
   ```

3. Tag the finished assignment and push the tag:

   ```bash
   git tag -a v1.0 -m "Spark 101 assignment complete"
   git push origin v1.0
   ```

4. Grab your commit graph for the final checkpoint in `ANSWERS.md`:

   ```bash
   git log --oneline --graph --all
   ```

---

## Tear down

```bash
docker compose down -v   # stops containers and deletes the mysql volume (resets seed data)
docker compose down      # stops containers, keeps data
```

## Notes

- Spark talks to MySQL on the Compose network hostname `mysql` (port 3306
  inside the network) — same pattern as `postgres` in the demo, different
  service name and driver.
- Host port `3306` is published only so you can poke at the data with a
  local MySQL client if you want to.
- Credentials: user `spark`, password `spark`, database `library`, root
  password `rootpw` (root is only for the healthcheck — use the `spark` user
  everywhere else).
- Do not commit `jars/mysql-connector-j.jar` — only `jars/.gitkeep` is
  tracked.

## Submission checklist

- [ ] 5 branches created, each with its own PR merged into `main`
- [ ] `ANSWERS.md` fully filled in (all checkpoints + reflection)
- [ ] `scripts/sparkcontext_and_session.py` — all 3 TODOs complete
- [ ] `scripts/library_analytics.py` — all 7 remaining queries written and run
- [ ] `v1.0` tag pushed
- [ ] Final PR (or repo link, if your program reviews via fork) shared with your instructor
