1890. The Latest Login in 2020

Table: Logins

+----------------+----------+
| Column Name    | Type     |
+----------------+----------+
| user_id        | int      |
| time_stamp     | datetime |
+----------------+----------+
(user_id, time_stamp) is the primary key (combination of columns with unique values) for this table.
Each row contains information about the login time for the user with ID user_id.
 

Write a solution to report the latest login for all users in the year 2020. Do not include the users who did not login in 2020.

Return the result table in any order.

The result format is in the following example.

Example 1:

Input: 
Logins table:
+---------+---------------------+
| user_id | time_stamp          |
+---------+---------------------+
| 6       | 2020-06-30 15:06:07 |
| 6       | 2021-04-21 14:06:06 |
| 6       | 2019-03-07 00:18:15 |
| 8       | 2020-02-01 05:10:53 |
| 8       | 2020-12-30 00:46:50 |
| 2       | 2020-01-16 02:49:50 |
| 2       | 2019-08-25 07:59:08 |
| 14      | 2019-07-14 09:00:00 |
| 14      | 2021-01-06 11:59:59 |
+---------+---------------------+
Output: 
+---------+---------------------+
| user_id | last_stamp          |
+---------+---------------------+
| 6       | 2020-06-30 15:06:07 |
| 8       | 2020-12-30 00:46:50 |
| 2       | 2020-01-16 02:49:50 |
+---------+---------------------+

WITH cte1 AS (
    SELECT
        user_id,
        time_stamp AS last_stamp,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time_stamp DESC) AS rnk
    FROM logins
    WHERE YEAR(time_stamp) = 2020
)
SELECT user_id, last_stamp
FROM cte1
WHERE rnk = 1;
--------------
SELECT user_id, MAX(time_stamp) AS last_stamp
FROM Logins
WHERE YEAR(time_stamp) = 2020
GROUP BY user_id;

least/last/first/max/greatest pr max/min click krna chahiye
---------------------------

from pyspark.sql.functions import year, col, max

result_df=df.filter(year("time_stamp")== 2020)\
    .groupBy("user_id")\
    .agg(max("time_stamp").alias("last_stamp"))
----------------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("user_id").orderBy(col("time_stamp").desc())

windowed_df = df_2020.withColumn("rnk", row_number().over(w))
#adding a new col with withColumn
result_window_df = windowed_df.filter(col("rnk") == 1).select("user_id", col("time_stamp").alias("last_stamp"))
