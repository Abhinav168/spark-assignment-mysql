CREATE TEMPORARY VIEW authors
USING org.apache.spark.sql.jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/library',
  driver 'com.mysql.cj.jdbc.Driver',
  dbtable 'authors',
  user 'spark',
  password 'spark'
);

CREATE TEMPORARY VIEW books
USING org.apache.spark.sql.jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/library',
  driver 'com.mysql.cj.jdbc.Driver',
  dbtable 'books',
  user 'spark',
  password 'spark'
);

CREATE TEMPORARY VIEW members
USING org.apache.spark.sql.jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/library',
  driver 'com.mysql.cj.jdbc.Driver',
  dbtable 'members',
  user 'spark',
  password 'spark'
);

CREATE TEMPORARY VIEW loans
USING org.apache.spark.sql.jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/library',
  driver 'com.mysql.cj.jdbc.Driver',
  dbtable 'loans',
  user 'spark',
  password 'spark'
);

CREATE TEMPORARY VIEW fines
USING org.apache.spark.sql.jdbc
OPTIONS (
  url 'jdbc:mysql://mysql:3306/library',
  driver 'com.mysql.cj.jdbc.Driver',
  dbtable 'fines',
  user 'spark',
  password 'spark'
);

SHOW TABLES;
SELECT COUNT(*) AS authors FROM authors;
SELECT COUNT(*) AS books FROM books;
