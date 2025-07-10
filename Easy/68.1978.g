1978. Employees Whose Manager Left the Company

Table: Employees

+-------------+----------+
| Column Name | Type     |
+-------------+----------+
| employee_id | int      |
| name        | varchar  |
| manager_id  | int      |
| salary      | int      |
+-------------+----------+

In SQL, employee_id is the primary key for this table.
This table contains information about the employees, their salary, and the ID of their manager. 
Some employees do not have a manager (manager_id is null). 

Find the IDs of the employees whose salary is strictly less than $30000 and whose manager left the company. 
When a manager leaves the company, their information is deleted from the Employees table, but the reports still have their manager_id set to the manager that left.

Return the result table ordered by employee_id.

The result format is in the following example.

Example 1:

Input:  
Employees table:
+-------------+-----------+------------+--------+
| employee_id | name      | manager_id | salary |
+-------------+-----------+------------+--------+
| 3           | Mila      | 9          | 60301  |
| 12          | Antonella | null       | 31000  |
| 13          | Emery     | null       | 67084  |
| 1           | Kalel     | 11         | 21241  |
| 9           | Mikaela   | null       | 50937  |
| 11          | Joziah    | 6          | 28485  |
+-------------+-----------+------------+--------+
Output: 
+-------------+
| employee_id |
+-------------+
| 11          |
+-------------+

Explanation: 
The employees with a salary less than $30000 are 1 (Kalel) and 11 (Joziah).
Kalel's manager is employee 11, who is still in the company (Joziah).
Joziah's manager is employee 6, who left the company because there is no row for employee 6 as it was deleted.

------------------------------------

SELECT e1.employee_id
FROM employees e1
LEFT JOIN employees e2 ON e1.manager_id = e2.employee_id
WHERE e1.salary < 30000
  AND e1.manager_id IS NOT NULL
  AND e2.employee_id IS NULL
ORDER BY e1.employee_id;

----------------------------

from pyspark.sql.functions import col

current_managers = df.select("employee_id").alias("mgrs")

filtered_employees = df.filter(
    (col("salary") < 30000) & (col("manager_id").isNotNull())
)

result_df = filtered_employees.join(
    current_managers,
    filtered_employees.manager_id == current_managers.employee_id,
    "left_anti"
).select("employee_id").orderBy("employee_id")

result_df.show()
-------------------------------
from pyspark.sql.functions import col

# Alias the DataFrame
e1 = df.alias("e1")
e2 = df.alias("e2")

joined_df = e1.join(
    e2,
    col("e1.manager_id") == col("e2.employee_id"),
    "left"
)

result_df = joined_df.filter(
    (col("e1.salary") < 30000) &
    (col("e1.manager_id").isNotNull()) &
    (col("e2.employee_id").isNull())
).select(col("e1.employee_id")).orderBy("employee_id")

result_df.show()