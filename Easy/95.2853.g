2853. Highest Salaries Difference
Description
Table: Salaries

+-------------+---------+ 
| Column Name | Type    | 
+-------------+---------+ 
| emp_name    | varchar | 
| department  | varchar | 
| salary      | int     |
+-------------+---------+
(emp_name, department) is the primary key for this table.
Each row of this table contains emp_name, department and salary. There will be at least one entry for the engineering and marketing departments.

Write an SQL query to calculate the difference between the highest salaries in the marketing and engineering department. Output the absolute difference in salaries.

Return the result table.

The query result format is in the following example.

 

Example 1:

Input: 
Salaries table:
+----------+-------------+--------+
| emp_name | department  | salary |
+----------+-------------+--------+
| Kathy    | Engineering | 50000  |
| Roy      | Marketing   | 30000  |
| Charles  | Engineering | 45000  |
| Jack     | Engineering | 85000  | 
| Benjamin | Marketing   | 34000  |
| Anthony  | Marketing   | 42000  |
| Edward   | Engineering | 102000 |
| Terry    | Engineering | 44000  |
| Evelyn   | Marketing   | 53000  |
| Arthur   | Engineering | 32000  |
+----------+-------------+--------+
Output: 
+-------------------+
| salary_difference | 
+-------------------+
| 49000             | 
+-------------------+
Explanation: 
- The Engineering and Marketing departments have the highest salaries of 102,000 and 53,000, respectively. Resulting in an absolute difference of 49,000.
-----------------------------------
MAX IGNORE NULLS, AND PUT NULL IF NOT PUT OThERHWISE OR ELSE 
------------------------------------

SELECT 
  ABS(
    MAX(CASE WHEN department = 'Engineering' THEN salary END) -
    MAX(CASE WHEN department = 'Marketing' THEN salary END)
  ) AS salary_difference
FROM Salaries;

---------------------------------

df=df.agg(max(when(col("department")=="Marketing"),"salary").alias("MAXENG"),max(when(col("department")=="Engineering"),"salary").alias("MAXMKT"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, max, abs

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    ("Kathy", "Engineering", 50000),
    ("Roy", "Marketing", 30000),
    ("Charles", "Engineering", 45000),
    ("Jack", "Engineering", 85000),
    ("Benjamin", "Marketing", 34000),
    ("Anthony", "Marketing", 42000),
    ("Edward", "Engineering", 102000),
    ("Terry", "Engineering", 44000),
    ("Evelyn", "Marketing", 53000),
    ("Arthur", "Engineering", 32000)
], ["emp_name", "department", "salary"])

agg_df = df.agg(
    max(when(col("department") == "Engineering", col("salary"))).alias("MAX_ENG"),
    max(when(col("department") == "Marketing", col("salary"))).alias("MAX_MKT")
)

result_df = agg_df.select(
    abs(col("MAX_ENG") - col("MAX_MKT")).alias("salary_difference")
)

salary_gap = result_df.collect()[0]["salary_difference"]

print(f"The salary difference is ₹{salary_gap}")

------------------
Think of agg() as a reducer: it can compress values, but it can't manipulate multiple results across the row during the same pass. Arithmetic across aggregates = post-processing step.

so this is wrong
df.agg(
    abs(
        max(when(col("department") == "Engineering", col("salary"))) -
        max(when(col("department") == "Marketing", col("salary")))
    ).alias("salary_difference")
)