# Spark 101 — Library Analytics on MySQL

See [take-home-assignment.md](take-home-assignment.md) for full instructions and [ANSWERS.md](ANSWERS.md) for the completed submission.

## Quick start

```bash
docker run --rm --user root -v "./jars:/jars" curlimages/curl:8.5.0 \
  -fsSL -o /jars/mysql-connector-j.jar \
  "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar"

docker compose up -d
docker compose exec mysql mysql -u spark -pspark library -e "SHOW TABLES;"
```
