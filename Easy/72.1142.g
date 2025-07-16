1142. User Activity for the Past 30 Days II
Table: Activity

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| user_id       | int     |
| session_id    | int     |
| activity_date | date    |
| activity_type | enum    |
+---------------+---------+
There is no primary key for this table, it may have duplicate rows.
The activity_type column is an ENUM of type ('open_session', 'end_session', 'scroll_down', 'send_message').
The table shows the user activities for a social media website.
Note that each session belongs to exactly one user.

 

Write an SQL query to find the average number of sessions per user for a period of 30 days ending 2019-07-27 inclusively, rounded to 2 decimal places. 
The sessions we want to count for a user are those with at least one activity in that time period.

The query result format is in the following example:

Activity table:
+---------+------------+---------------+---------------+
| user_id | session_id | activity_date | activity_type |
+---------+------------+---------------+---------------+
| 1       | 1          | 2019-07-20    | open_session  |
| 1       | 1          | 2019-07-20    | scroll_down   |
| 1       | 1          | 2019-07-20    | end_session   |
| 2       | 4          | 2019-07-20    | open_session  |
| 2       | 4          | 2019-07-21    | send_message  |
| 2       | 4          | 2019-07-21    | end_session   |
| 3       | 2          | 2019-07-21    | open_session  |
| 3       | 2          | 2019-07-21    | send_message  |
| 3       | 2          | 2019-07-21    | end_session   |
| 3       | 5          | 2019-07-21    | open_session  |
| 3       | 5          | 2019-07-21    | scroll_down   |
| 3       | 5          | 2019-07-21    | end_session   |
| 4       | 3          | 2019-06-25    | open_session  |
| 4       | 3          | 2019-06-25    | end_session   |
+---------+------------+---------------+---------------+

Result table:
+---------------------------+
| average_sessions_per_user |
+---------------------------+
| 1.33                      |
+---------------------------+
User 1 and 2 each had 1 session in the past 30 days while user 3 had 2 sessions so the average is 

--------------------
language kahcra hai question ki, what we actually care about is acutal number of sessions, and actual number of users
then avg ession per user
-------------------------------

select round(count(DISTINCT session_id)*1.0/(count(DISTINCT user_id*1.0)),2)
from activity
where activity_date between 2019-07-27 - INTERVAL 29 DAY AND 2019-07-27;

OR

SELECT 
  ROUND(COUNT(DISTINCT session_id) * 1.0 / COUNT(DISTINCT user_id), 2) AS average_sessions_per_user
FROM activity
WHERE activity_date BETWEEN DATE_SUB('2019-07-27', INTERVAL 29 DAY) AND '2019-07-27';

----------------------------

from pyspark.sql import functions as F

# Step 1: Filter activities to the 30-day window
filtered = activity_df.filter(
    (F.col("activity_date") >= F.lit("2019-06-28")) & 
    (F.col("activity_date") <= F.lit("2019-07-27"))
)

# Step 2: Count distinct sessions and distinct users
distinct_sessions_df = filtered.select("session_id").distinct()
distinct_users_df = filtered.select("user_id").distinct()

# Step 3: Cross aggregate using DataFrame math (keeping it all in PySpark)
agg_df = distinct_sessions_df.count()  # total sessions
user_df = distinct_users_df.count()    # total users

# Build single-row DataFrame to hold the result
result_df = spark.createDataFrame(
    [(round(agg_df / user_df, 2))],
    ["average_sessions_per_user"]
)

result_df.show()