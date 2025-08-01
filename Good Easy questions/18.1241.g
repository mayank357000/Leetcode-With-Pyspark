1241. Number of Comments per Post
Table: Submissions

+---------------+----------+
| sub_id        | int      |
| parent_id     | int      |
+---------------+----------+
There is no primary key for this table, it may have duplicate rows.
Each row can be a post or comment on the post.
parent_id is null for posts.
parent_id for comments is sub_id for another post in the table.
 

Write an SQL query to find number of comments per each post.


Result table should contain post_id and its corresponding number_of_comments, and must be sorted by post_id in ascending order.

Submissions may contain duplicate comments. You should count the number of unique comments per post.

Submissions may contain duplicate posts. You should treat them as one post.

The query result format is in the following example:

Submissions table:
+---------+------------+
| sub_id  | parent_id  |
+---------+------------+
| 1       | Null       |
| 2       | Null       |
| 1       | Null       |
| 12      | Null       |
| 3       | 1          |
| 5       | 2          |
| 3       | 1          |
| 4       | 1          |
| 9       | 1          |
| 10      | 2          |
| 6       | 7          |
+---------+------------+

Result table:
+---------+--------------------+
| post_id | number_of_comments |
+---------+--------------------+
| 1       | 3                  |
| 2       | 2                  |
| 12      | 0                  |
+---------+--------------------+

The post with id 1 has three comments in the table with id 3, 4 and 9. The comment with id 3 is repeated in the table, we counted it only once.
The post with id 2 has two comments in the table with id 5 and 10.
The post with id 12 has no comments in the table.
The comment with id 6 is a comment on a deleted post with id 7 so we ignored it.
--------------------
->get distinct post_id and all of them
->then get count of comments per posts
->if some posts had no comments then 0

WITH cte1 AS (
    -- Get distinct posts
    SELECT DISTINCT sub_id AS post_id 
    FROM Submissions 
    WHERE parent_id IS NULL
),
cte2 AS (
    -- Count distinct comments per post
    SELECT parent_id, COUNT(DISTINCT sub_id) AS num_comments
    FROM Submissions
    WHERE parent_id IS NOT NULL
    GROUP BY parent_id
)
SELECT 
    cte1.post_id, 
    COALESCE(cte2.num_comments, 0) AS number_of_comments
FROM cte1
LEFT JOIN cte2 ON cte1.post_id = cte2.parent_id
ORDER BY cte1.post_id;

---------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, coalesce

# Initialize Spark Session
spark = SparkSession.builder.appName("CommentsPerPost").getOrCreate()

# Sample Data, null is None in python 
data = [
    (1, None), (2, None), (1, None), (12, None),
    (3, 1), (5, 2), (3, 1), (4, 1), (9, 1), (10, 2), (6, 7)
]

columns = ["sub_id", "parent_id"]
df = spark.createDataFrame(data, columns)

posts_df = df.filter(col("parent_id").isNull()).select(col("sub_id").alias("post_id")).distinct()

comments_df = df.filter(col("parent_id").isNotNull()) \
                .groupBy("parent_id").agg(countDistinct("sub_id").alias("number_of_comments"))

result_df = posts_df.join(comments_df, posts_df.post_id == comments_df.parent_id, "left") \
            .select(posts_df.post_id, coalesce(col("number_of_comments"), 0).alias("number_of_comments"))

#OR

result_df = posts_df.join(comments_df, posts_df.post_id == comments_df.parent_id, "left") \
    .select(posts_df.post_id, \
            when(col("number_of_comments").isNotNull(), col("number_of_comments")).otherwise(0).alias("number_of_comments"))

result_df.orderBy("post_id").show()
