1789. Primary Department for Each Employee
Easy
Topics
premium lock icon
Companies
SQL Schema
Pandas Schema
Table: Employee

+---------------+---------+
| Column Name   |  Type   |
+---------------+---------+
| employee_id   | int     |
| department_id | int     |
| primary_flag  | varchar |
+---------------+---------+
(employee_id, department_id) is the primary key (combination of columns with unique values) for this table.
employee_id is the id of the employee.
department_id is the id of the department to which the employee belongs.
primary_flag is an ENUM (category) of type ('Y', 'N'). If the flag is 'Y', the department is the primary department for the employee. If the flag is 'N', the department is not the primary.
 

Employees can belong to multiple departments. When the employee joins other departments, they need to decide which department is their primary department. Note that when an employee belongs to only one department, their primary column is 'N'.

Write a solution to report all the employees with their primary department. For employees who belong to one department, report their only department.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employee table:
+-------------+---------------+--------------+
| employee_id | department_id | primary_flag |
+-------------+---------------+--------------+
| 1           | 1             | N            |
| 2           | 1             | Y            |
| 2           | 2             | N            |
| 3           | 3             | N            |
| 4           | 2             | N            |
| 4           | 3             | Y            |
| 4           | 4             | N            |
+-------------+---------------+--------------+
Output: 
+-------------+---------------+
| employee_id | department_id |
+-------------+---------------+
| 1           | 1             |
| 2           | 1             |
| 3           | 3             |
| 4           | 3             |
+-------------+---------------+
Explanation: 
- The Primary department for employee 1 is 1.
- The Primary department for employee 2 is 1.
- The Primary department for employee 3 is 3.
- The Primary department for employee 4 is 3.

-------------------------------------------------
understand the question well
------------------------------------------------
SELECT employee_id, department_id
FROM Employee
WHERE primary_flag = 'Y'

UNION

SELECT employee_id, department_id
FROM Employee
WHERE primary_flag = 'N'
  AND employee_id IN (
      SELECT employee_id
      FROM Employee
      GROUP BY employee_id
      HAVING COUNT(*) = 1
  );
#mathicng col hi null hoga when left join not mathced on right table

OR

SELECT DISTINCT s.employee_id
FROM Employee s
JOIN Product p ON s.department_id = p.department_id
WHERE p.primary_flag = 'Y'
  AND NOT EXISTS (
      SELECT 1
      FROM Employee s2
      JOIN Product p2 ON s2.department_id = p2.department_id
      WHERE p2.primary_flag = 'N'
        AND s2.employee_id = s.employee_id
  );
#subquery ka use, not exists work better than not in all cases

------------------------------------

from pyspark.sql.functions import col, count

# Step 1: Employees with primary department marked 'Y'
df1 = df.filter(col("primary_flag") == 'Y') \
        .select("employee_id", "department_id")

# Step 2: Employees who belong to only one department (with 'N' flag)
# First, find employees with only one department
single_dept_ids = df.groupBy("employee_id") \
                    .agg(count("*").alias("dept_count")) \
                    .filter(col("dept_count") == 1)

# Then join back to original to get department_id for those who have 'N'
df2 = df.filter(col("primary_flag") == 'N') \
        .join(single_dept_ids, on="employee_id") \
        .select("employee_id", "department_id")

# Final union
result_df = df1.union(df2)

--------
so get single dept emp_id then check which N having id is part of that single dept list
