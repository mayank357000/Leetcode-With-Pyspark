550. Game Play Analysis IV
Table: Activity

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| player_id    | int     |
| device_id    | int     |
| event_date   | date    |
| games_played | int     |
+--------------+---------+

(player_id, event_date) is the primary key (combination of columns with unique values) of this table.
This table shows the activity of players of some games.
Each row is a record of a player who logged in and played a 
number of games (possibly 0) before logging out on someday using some device.

Write a solution to report the fraction of players that logged 
in again on the day after the day they first logged in, rounded to 2 decimal places.
In other words, you need to determine the number of players who 
logged in on the day immediately following their initial login, 
and divide it by the number of total players.

The result format is in the following example.

Example 1:

Input: 
Activity table:
+-----------+-----------+------------+--------------+
| player_id | device_id | event_date | games_played |
+-----------+-----------+------------+--------------+
| 1         | 2         | 2016-03-01 | 5            |
| 1         | 2         | 2016-03-02 | 6            |
| 2         | 3         | 2017-06-25 | 1            |
| 3         | 1         | 2016-03-02 | 0            |
| 3         | 4         | 2018-07-03 | 5            |
+-----------+-----------+------------+--------------+
Output: 
+-----------+
| fraction  |
+-----------+
| 0.33      |
+-----------+
Explanation: 
Only the player with id 1 logged back in after the first day he had logged in so the answer is 1/3 = 0.33

-----------------
to get division done here we cross join the table with single col and did that
----------------------
WITH FirstLogin AS (
  SELECT player_id, MIN(event_date) AS first_login
  FROM Activity
  GROUP BY player_id
),
NextDayLogin AS (
  SELECT a.player_id
  FROM Activity a
  JOIN FirstLogin f
    ON a.player_id = f.player_id
   AND a.event_date = DATE_ADD(f.first_login, INTERVAL 1 DAY)
),
TotalPlayers AS (
  SELECT COUNT(DISTINCT player_id) AS total_players
  FROM Activity
),
ReturningPlayers AS (
  SELECT COUNT(DISTINCT player_id) AS returning_players
  FROM NextDayLogin
),
FinalFraction AS (
  SELECT 
    r.returning_players * 1.0 / t.total_players AS fraction
  FROM ReturningPlayers r
  JOIN TotalPlayers t ON 1=1
)
SELECT ROUND(fraction, 2) AS fraction
FROM FinalFraction;

OR

# Write your MySQL query statement below
with cte as
(select *,
row_number() over(partition by player_id order by event_date) as rnk
from activity),
cte2 as
(
    select c1.player_id
    from cte c1 join cte c2
    on c1.player_id=c2.player_id and c1.rnk=1 and c2.rnk=2 and c1.event_date +  interval 1 day=c2.event_date
)
select round((select count(*) from cte2)/(select count(distinct player_id) from activity),2) as fraction;

-----------------------------

from pyspark.sql import functions as F

first_login_df = (
    df.groupBy("player_id")
      .agg(F.min("event_date").alias("first_login"))
)

first_login_df = first_login_df.withColumn("next_day", F.date_add("first_login", 1))

next_day_login_df = (
    df.join(first_login_df,
            (df["player_id"] == first_login_df["player_id"]) &
            (df["event_date"] == first_login_df["next_day"]),
            "inner")
      .select(df["player_id"])
      .distinct()
)

total_players = df.select("player_id").distinct().count()
returning_players = next_day_login_df.count()

fraction = round(returning_players / total_players, 2)

result_df = spark.createDataFrame([(fraction,)], ["fraction"])
result_df.show()