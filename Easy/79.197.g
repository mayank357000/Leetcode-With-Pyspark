197. Rising Temperature
Easy
Topics
premium lock icon
Companies
SQL Schema
Pandas Schema
Table: Weather

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| recordDate    | date    |
| temperature   | int     |
+---------------+---------+
id is the column with unique values for this table.
There are no different rows with the same recordDate.
This table contains information about the temperature on a certain day.
 

Write a solution to find all dates' id with higher temperatures compared to its previous dates (yesterday).

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Weather table:
+----+------------+-------------+
| id | recordDate | temperature |
+----+------------+-------------+
| 1  | 2015-01-01 | 10          |
| 2  | 2015-01-02 | 25          |
| 3  | 2015-01-03 | 20          |
| 4  | 2015-01-04 | 30          |
+----+------------+-------------+
Output: 
+----+
| id |
+----+
| 2  |
| 4  |
+----+
Explanation: 
In 2015-01-02, the temperature was higher than the previous day (10 -> 25).
In 2015-01-04, the temperature was higher than the previous day (20 -> 30).

-----------------------------------
no group here , full tbale a group so no partitionBy
------------------------------------
SELECT id
FROM (
    SELECT 
        id,
        temperature,
        LAG(temperature) OVER (ORDER BY recordDate) AS prev_temp
    FROM Weather
) sub
WHERE temperature > prev_temp;

---------------------------------------
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

window_spec = Window.orderBy("recordDate")

weather_df = weather_df.withColumn(
    "prev_temp", lag("temperature").over(window_spec)
)

result_df = weather_df.filter(col("temperature") > col("prev_temp")).select("id")