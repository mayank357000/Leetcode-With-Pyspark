1355. Activity Participants
SQL Schema 
Table: Friends

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| name          | varchar |
| activity      | varchar |
+---------------+---------+
id is the id of the friend and primary key for this table.
name is the name of the friend.
activity is the name of the activity which the friend takes part in
Table: Activities

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| name          | varchar |
+---------------+---------+
id is the primary key for this table
name is the name of the activity

Write an SQL query to find the names of all the activities with neither maximum, nor minimum number of participants.

Return the result table in any order. Each activity in table Activities is performed by any person in the table Friends.

The query result format is in the following example:

Friends table:
+------+--------------+---------------+
| id   | name         | activity      |
+------+--------------+---------------+
| 1    | Jonathan D.  | Eating        |
| 2    | Jade W.      | Singing       |
| 3    | Victor J.    | Singing       |
| 4    | Elvis Q.     | Eating        |
| 5    | Daniel A.    | Eating        |
| 6    | Bob B.       | Horse Riding  |
+------+--------------+---------------+

Activities table:
+------+--------------+
| id   | name         |
+------+--------------+
| 1    | Eating       |
| 2    | Singing      |
| 3    | Horse Riding |
+------+--------------+

Result table:
+--------------+
| results      |
+--------------+
| Singing      |
+--------------+

Eating activity is performed by 3 friends, maximum number of participants, (Jonathan D. , Elvis Q. and Daniel A.)
Horse Riding activity is performed by 1 friend, minimum number of participants, (Bob B.)
Singing is performed by 2 friends (Victor J. and Jade W.)

-------------------------------

WITH cte AS (
    SELECT activity, COUNT(*) AS cnt
    FROM Friends
    GROUP BY activity
)
SELECT activity AS results
FROM cte
WHERE cnt NOT IN (
    SELECT MAX(cnt) FROM cte
    UNION
    SELECT MIN(cnt) FROM cte
);

#max return a single value if used outside groupby
----------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, min

spark = SparkSession.builder.getOrCreate()

# Sample Friends DataFrame
friends_df = spark.createDataFrame([
    ("Jonathan", "Eating"),
    ("Jade", "Singing"),
    ("Victor", "Singing"),
    ("Elvis", "Eating"),
    ("Daniel", "Eating"),
    ("Bob", "Horse Riding")
], ["name", "activity"])

activity_counts = friends_df.groupBy("activity").agg(count("*").alias("cnt"))

max_count = activity_counts.select(max("cnt")).first()[0]
min_count = activity_counts.select(min("cnt")).first()[0]
#can use this too: activity_counts.select(max("cnt")).collect()[0][0]
#this collect() returns list of row object,we get first row and either use index or "string key" to get col value

result_df = activity_counts.filter(
    (col("cnt") != lit(max_count)) & (col("cnt") != lit(min_count))
).select("activity")

final_df = result_df.withColumnRenamed("activity", "results")

final_df.show()