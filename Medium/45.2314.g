2314. The First Day of the Maximum Recorded Degree in Each City
Description
Table: Weather

+-------------+------+
| Column Name | Type |
+-------------+------+
| city_id     | int  |
| day         | date |
| degree      | int  |
+-------------+------+

(city_id, day) is the primary key for this table.
Each row in this table contains the degree of the weather of a city on a certain day.
All the degrees are recorded in the year 2022.

Write an SQL query to report the day that has the maximum recorded degree in each city. 
If the maximum degree was recorded for the same city multiple times, 
return the earliest day among them.

Return the result table ordered by city_id in ascending order.

The query result format is shown in the following example.

Example 1:

Input: 
Weather table:
+---------+------------+--------+
| city_id | day        | degree |
+---------+------------+--------+
| 1       | 2022-01-07 | -12    |
| 1       | 2022-03-07 | 5      |
| 1       | 2022-07-07 | 24     |
| 2       | 2022-08-07 | 37     |
| 2       | 2022-08-17 | 37     |
| 3       | 2022-02-07 | -7     |
| 3       | 2022-12-07 | -6     |
+---------+------------+--------+

Output: 
+---------+------------+--------+
| city_id | day        | degree |
+---------+------------+--------+
| 1       | 2022-07-07 | 24     |
| 2       | 2022-08-07 | 37     |
| 3       | 2022-12-07 | -6     |
+---------+------------+--------+

Explanation: 
For city 1, the maximum degree was recorded on 2022-07-07 with 24 degrees.
For city 1, the maximum degree was recorded on 2022-08-07 and 2022-08-17 
with 37 degrees. We choose the earlier date (2022-08-07).
For city 3, the maximum degree was recorded on 2022-12-07 with -6 degrees.

---------------------------------

WITH ranked_weather AS (
    SELECT 
        city_id,
        day,
        degree,
        ROW_NUMBER() OVER (
            PARTITION BY city_id 
            ORDER BY degree DESC, day ASC
        ) AS rn
    FROM Weather
)
SELECT city_id, day, degree
FROM ranked_weather
WHERE rn = 1
ORDER BY city_id;

OR

SELECT 
    city_id,
    MIN(day) AS day,
    MAX(degree) AS degree
FROM Weather
GROUP BY city_id
HAVING MAX(degree) = ANY (
    SELECT degree
    FROM Weather w2
    WHERE w2.city_id = Weather.city_id
)

-------------------------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = Window.partitionBy("city_id").orderBy(F.col("degree").desc(), F.col("day").asc())

ranked_weather_df = (
    weather_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .select("city_id", "day", "degree")
    .orderBy("city_id")
)

OR
#INNER JOIN TO GET MAX DEGREE DATES PER CITY, THEN GET MIN_DAY
from pyspark.sql import functions as F

max_degree_df = (
    weather_df
    .groupBy("city_id")
    .agg(F.max("degree").alias("max_degree"))
)

max_days_df = (
    weather_df
    .join(max_degree_df, on=["city_id"])
    .filter(F.col("degree") == F.col("max_degree"))
)

final_df = (
    max_days_df
    .groupBy("city_id", "degree")
    .agg(F.min("day").alias("day"))
    .select("city_id", "day", "degree")
    .orderBy("city_id")
)