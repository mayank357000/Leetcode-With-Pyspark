1951. All the Pairs With the Maximum Number of Common Followers
Level
Medium

Description
Table: Relations

+-------------+------+
| Column Name | Type |
+-------------+------+
| user_id     | int  |
| follower_id | int  |
+-------------+------+
(user_id, follower_id) is the primary key for this table.
Each row of this table indicates that the user with ID follower_id is following the user with ID user_id.
Write an SQL query to find all the pairs of users with the maximum number of common followers. 
In other words, if the maximum number of common followers between any two users is maxCommon, then you have to return all pairs of users that have maxCommon common followers.

The result table should contain the pairs user1_id and user2_id where user1_id < user2_id.

Return the result table in any order.

The query result format is in the following example:

Relations table:
+---------+-------------+
| user_id | follower_id |
+---------+-------------+
| 1       | 3           |
| 2       | 3           |
| 7       | 3           |
| 1       | 4           |
| 2       | 4           |
| 7       | 4           |
| 1       | 5           |
| 2       | 6           |
| 7       | 5           |
+---------+-------------+

Result table:
+----------+----------+
| user1_id | user2_id |
+----------+----------+
| 1        | 7        |
+----------+----------+

Users 1 and 2 have 2 common followers (3 and 4).
Users 1 and 7 have 3 common followers (3, 4, and 5).
Users 2 and 7 have 2 common followers (3 and 4).
Since the maximum number of common followers between 
any two users is 3, we return all pairs of users with 
3 common followers, which is only the pair (1, 7). 
We return the pair as (1, 7), not as (7, 1).
Note that we do not have any information about the 
users that follow users 3, 4, and 5, so we consider
them to have 0 followers.

WITH cte1 AS (
    SELECT 
        r1.user_id AS user1_id,
        r2.user_id AS user2_id,
        COUNT(*) AS common_followers
    FROM relations r1
    JOIN relations r2 
        ON r1.follower_id = r2.follower_id
    WHERE r1.user_id < r2.user_id  -- "avoids self-pairs and duplicates"
    GROUP BY r1.user_id, r2.user_id
),
cte2 AS (
    SELECT MAX(common_followers) AS max_common FROM cte1
)
SELECT user1_id, user2_id
FROM cte1
JOIN cte2 ON cte1.common_followers = cte2.max_common;

------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (1, 3), (2, 3), (7, 3),
    (1, 4), (2, 4), (7, 4),
    (1, 5), (2, 6), (7, 5)
]
columns = ["user_id", "follower_id"]

relations = spark.createDataFrame(data, columns)

joined = relations.alias("r1").join(
    relations.alias("r2"),
    on="follower_id"
).filter(col("r1.user_id") < col("r2.user_id"))

common_counts = joined.groupBy(
    col("r1.user_id").alias("user1_id"),
    col("r2.user_id").alias("user2_id")
).agg(count("*").alias("common_followers"))

#can use window function or join with max table with broadcast
max_common = common_counts.agg(spark_max("common_followers").alias("max_common")).collect()[0]["max_common"]

result = common_counts.filter(col("common_followers") == max_common)

result.select("user1_id", "user2_id").show()