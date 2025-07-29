1699. Number of Calls Between Two Persons
SQL Schema 
Table: Calls

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| from_id     | int     |
| to_id       | int     |
| duration    | int     |
+-------------+---------+
This table does not have a primary key, it may contain duplicates.
This table contains the duration of a phone call between from_id and to_id.
from_id != to_id

Write an SQL query to report the number of calls and the total call duration between each pair of distinct persons (person1, person2) where person1 < person2.

Return the result table in any order.

The query result format is in the following example:

Calls table:
+---------+-------+----------+
| from_id | to_id | duration |
+---------+-------+----------+
| 1       | 2     | 59       |
| 2       | 1     | 11       |
| 1       | 3     | 20       |
| 3       | 4     | 100      |
| 3       | 4     | 200      |
| 3       | 4     | 200      |
| 4       | 3     | 499      |
+---------+-------+----------+

Result table:
+---------+---------+------------+----------------+
| person1 | person2 | call_count | total_duration |
+---------+---------+------------+----------------+
| 1       | 2       | 2          | 70             |
| 1       | 3       | 1          | 20             |
| 3       | 4       | 4          | 999            |
+---------+---------+------------+----------------+
Users 1 and 2 had 2 calls and the total duration is 70 (59 + 11).
Users 1 and 3 had 1 call and the total duration is 20.
Users 3 and 4 had 4 calls and the total duration is 999 (100 + 200 + 200 + 499).

------------------------------
distinct pair by col1<col2 or one pair onnly by least, greatest func which takes col values
----------------------------------

WITH cte1 AS (
  SELECT from_id AS person1, to_id AS person2, duration FROM Calls
  UNION ALL
  SELECT to_id AS person1, from_id AS person2, duration FROM Calls
)
SELECT 
  person1, 
  person2,
  COUNT(*) AS call_count,
  SUM(duration) AS total_duration
FROM cte1
WHERE person1 < person2
GROUP BY person1, person2;

or

WITH unified_calls AS (
    SELECT 
        LEAST(from_id, to_id) AS person1,
        GREATEST(from_id, to_id) AS person2,
        duration
    FROM Calls
)
SELECT 
    person1, 
    person2,
    COUNT(*) AS call_count,
    SUM(duration) AS total_duration
FROM unified_calls
GROUP BY person1, person2;

----------------------

from pyspark.sql import functions as F

normalized_df = (
    calls_df
    .withColumn("person1", F.least(F.col("from_id"), F.col("to_id")))
    .withColumn("person2", F.greatest(F.col("from_id"), F.col("to_id")))
    .groupBy("person1", "person2")
    .agg(
        F.count("*").alias("call_count"),
        F.sum("duration").alias("total_duration")
    )
)

OR

from pyspark.sql import functions as F

calls_a = calls_df.select(
    F.col("from_id").alias("person1"),
    F.col("to_id").alias("person2"),
    "duration"
)

calls_b = calls_df.select(
    F.col("to_id").alias("person1"),
    F.col("from_id").alias("person2"),
    "duration"
)

union_df = calls_a.unionByName(calls_b)

filtered_df = union_df.filter(F.col("person1") < F.col("person2"))

result_df = filtered_df.groupBy("person1", "person2").agg(
    F.count("*").alias("call_count"),
    F.sum("duration").alias("total_duration")
)