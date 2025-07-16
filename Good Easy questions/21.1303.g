1303. Find the Team Size

SQL Schema 
Table: Employee

+---------------+---------+
| employee_id   | int     |
| team_id       | int     |
+---------------+---------+
employee_id is the primary key for this table.
Each row of this table contains the ID of each employee and their respective team.
Write an SQL query to find the team size of each of the employees.

Return result table in any order.

The query result format is in the following example:

Employee Table:
+-------------+------------+
| employee_id | team_id    |
+-------------+------------+
|     1       |     8      |
|     2       |     8      |
|     3       |     8      |
|     4       |     7      |
|     5       |     9      |
|     6       |     9      |
+-------------+------------+

Result table:
+-------------+------------+
| employee_id | team_size  |
+-------------+------------+
|     1       |     3      |
|     2       |     3      |
|     3       |     3      |
|     4       |     1      |
|     5       |     2      |
|     6       |     2      |
+-------------+------------+
Employees with Id 1,2,3 are part of a team with team_id = 8.
Employees with Id 4 is part of a team with team_id = 7.
Employees with Id 5,6 are part of a team with team_id = 9.

-------------------------
SELECT 
    employee_id,
    COUNT(*) OVER (PARTITION BY team_id) AS team_size
FROM Employee;

#count(distinct column_name)

#alt and basic approach of grp by get count then join
---------------------------
from pyspark.sql.functions import count, col

ts_df = e_df.groupBy("team_id").agg(count("employee_id").alias("team_size"))

result_df = e_df.join(ts_df, e_df.team_id == ts_df.team_id, "left") \
    .select(e_df.employee_id, ts_df.team_size)

OR

result_df = e_df.join(ts_df, on="team_id", how="left") \
    .select(e_df.employee_id, ts_df.team_size)

result_df.show()

OR

w = Window.partitionBy("team_id")

# Add the team_size column using the window function
result_df = e_df.withColumn("team_size", count("employee_id").over(w)) \
    .select("employee_id", "team_size")
