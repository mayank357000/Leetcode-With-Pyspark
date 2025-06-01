1873. Calculate Special Bonus
Solved
Easy
Topics
premium lock icon
Companies
SQL Schema
Pandas Schema
Table: Employees

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| employee_id | int     |
| name        | varchar |
| salary      | int     |
+-------------+---------+
employee_id is the primary key (column with unique values) for this table.
Each row of this table indicates the employee ID, employee name, and salary.
 

Write a solution to calculate the bonus of each employee. The bonus of an employee is 100% of their salary if the ID of the employee is an odd number and the employee's name does not start with the character 'M'. The bonus of an employee is 0 otherwise.

Return the result table ordered by employee_id.

The result format is in the following example.

 

Example 1:

Input: 
Employees table:
+-------------+---------+--------+
| employee_id | name    | salary |
+-------------+---------+--------+
| 2           | Meir    | 3000   |
| 3           | Michael | 3800   |
| 7           | Addilyn | 7400   |
| 8           | Juan    | 6100   |
| 9           | Kannon  | 7700   |
+-------------+---------+--------+
Output: 
+-------------+-------+
| employee_id | bonus |
+-------------+-------+
| 2           | 0     |
| 3           | 0     |
| 7           | 7400  |
| 8           | 0     |
| 9           | 7700  |
+-------------+-------+

SELECT
    employee_id,
    CASE
        WHEN MOD(employee_id, 2) = 1 AND LEFT(name, 1) <> 'M' THEN salary
        ELSE 0
    END AS bonus
FROM Employees
ORDER BY employee_id;

OR

SELECT employee_id,
       IF(name NOT LIKE 'M%' AND MOD(employee_id, 2) <> 0, salary, 0) AS bonus
FROM employees
ORDER BY employee_id;

---------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("SpecialBonus").getOrCreate()

# Sample data
data = [
    (2, "Meir", 3000),
    (3, "Michael", 3800),
    (7, "Addilyn", 7400),
    (8, "Juan", 6100),
    (9, "Kannon", 7700)
]
columns = ["employee_id", "name", "salary"]

df = spark.createDataFrame(data, columns)

result_df = df.withColumn(
    "bonus",
    when(
        (col("employee_id") % 2 == 1) & (~col("name").startswith("M")),
        col("salary")
    ).otherwise(0)
).select("employee_id", "bonus").orderBy("employee_id")

OR

df.filter(col("name").like("M%"))

result_df.show()