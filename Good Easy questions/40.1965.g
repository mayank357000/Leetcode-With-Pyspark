1965. Employees With Missing Information
Table: Employees

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| employee_id | int     |
| name        | varchar |
+-------------+---------+
employee_id is the column with unique values for this table.
Each row of this table indicates the name of the employee whose ID is employee_id.
 

Table: Salaries

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| employee_id | int     |
| salary      | int     |
+-------------+---------+
employee_id is the column with unique values for this table.
Each row of this table indicates the salary of the employee whose ID is employee_id.
 

Write a solution to report the IDs of all the employees with missing information. The information of an employee is missing if:

The employee's name is missing, or
The employee's salary is missing.
Return the result table ordered by employee_id in ascending order.

The result format is in the following example.

 

Example 1:

Input: 
Employees table:
+-------------+----------+
| employee_id | name     |
+-------------+----------+
| 2           | Crew     |
| 4           | Haven    |
| 5           | Kristian |
+-------------+----------+
Salaries table:
+-------------+--------+
| employee_id | salary |
+-------------+--------+
| 5           | 76071  |
| 1           | 22517  |
| 4           | 63539  |
+-------------+--------+
Output: 
+-------------+
| employee_id |
+-------------+
| 1           |
| 2           |
+-------------+

-----------------------------
UNDERSTAND THE PROBLEM CORRECTLY
what we need is no slaary or no name emp, 
"emp table id if not presnt in salary means salary missing for that emp and vice versa"
so simple union won't work to detect missing other table info
-----------------------------

SELECT e.employee_id
FROM Employees e
FULL OUTER JOIN Salaries s
  ON e.employee_id = s.employee_id
WHERE e.name IS NULL OR s.salary IS NULL
ORDER BY employee_id;

------------------------------

we need eid whose either name is null or salary null
if we do separate salary null and name null union, we might might id who is not present in other table
union just remove duplicates if they exist, not get us common rows

#get all salary null= all salary null values + salary table ids not present in employee table

SELECT e.employee_id
FROM Employees e
LEFT JOIN Salaries s ON e.employee_id = s.employee_id
WHERE s.salary IS NULL

UNION

#get all name null = all name null values + employee table ids not present in salary table

SELECT s.employee_id
FROM Salaries s
LEFT JOIN Employees e ON s.employee_id = e.employee_id
WHERE e.name IS NULL

ORDER BY employee_id;

##query union query order by xyz;

-------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("MissingEmployeeInfo").getOrCreate()

# Sample data
employees_data = [(2, "Crew"), (4, "Haven"), (5, "Kristian")]
salaries_data = [(5, 76071), (1, 22517), (4, 63539)]

employees_df = spark.createDataFrame(employees_data, ["employee_id", "name"])
salaries_df = spark.createDataFrame(salaries_data, ["employee_id", "salary"])

# Full outer join and filter where name or salary is null
result_df = employees_df.join(
    salaries_df, on="employee_id", how="outer"
).filter(
    (col("name").isNull()) | (col("salary").isNull())
).select("employee_id").orderBy("employee_id")

result_df.show()
---------------------------

missing_salary = employees_df.join(
    salaries_df, on="employee_id", how="left"
).filter(
    col("salary").isNull()
).select("employee_id")

missing_name = salaries_df.join(
    employees_df, on="employee_id", how="left"
).filter(
    col("name").isNull()
).select("employee_id")

result_df = missing_salary.union(missing_name).orderBy("employee_id")
result_df.show()
