1907. Count Salary Categories
Table: Accounts

+-------------+------+
| Column Name | Type |
+-------------+------+
| account_id  | int  |
| income      | int  |
+-------------+------+
account_id is the primary key (column with unique values) for this table.
Each row contains information about the monthly income for one bank account.

Write a solution to calculate the number of bank accounts for each salary category.
The salary categories are:

"Low Salary": All the salaries strictly less than $20000.
"Average Salary": All the salaries in the inclusive range [$20000, $50000].
"High Salary": All the salaries strictly greater than $50000.
The result table must contain all three categories. If there are no accounts in a category, return 0.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Accounts table:
+------------+--------+
| account_id | income |
+------------+--------+
| 3          | 108939 |
| 2          | 12747  |
| 8          | 87709  |
| 6          | 91796  |
+------------+--------+
Output: 
+----------------+----------------+
| category       | accounts_count |
+----------------+----------------+
| Low Salary     | 1              |
| Average Salary | 0              |
| High Salary    | 3              |
+----------------+----------------+
Explanation: 
Low Salary: Account 2.
Average Salary: No accounts.
High Salary: Accounts 3, 6, and 8.

--------------------------------

SELECT 'Low Salary' AS category, 
       SUM(CASE WHEN income < 20000 THEN 1 ELSE 0 END) AS accounts_count
FROM Accounts

UNION ALL

SELECT 'Average Salary' AS category, 
       SUM(CASE WHEN income BETWEEN 20000 AND 50000 THEN 1 ELSE 0 END)
FROM Accounts

UNION ALL

SELECT 'High Salary' AS category, 
       SUM(CASE WHEN income > 50000 THEN 1 ELSE 0 END)
FROM Accounts;

OR

WITH categorized_accounts AS (
  SELECT
    *,
    CASE
      WHEN income < 20000 THEN 'Low Salary'
      WHEN income BETWEEN 20000 AND 50000 THEN 'Average Salary'
      ELSE 'High Salary'
    END AS category
  FROM Accounts
)

SELECT
  category,
  COUNT(*) AS accounts_count
FROM categorized_accounts
GROUP BY category;

-----------
from pyspark.sql.functions import lit

low_df = accounts_df.filter(col("income") < 20000) \
    .select(lit("Low Salary").alias("category")) \
    .groupBy("category").count()

avg_df = accounts_df.filter((col("income") >= 20000) & (col("income") <= 50000)) \
    .select(lit("Average Salary").alias("category")) \
    .groupBy("category").count()

high_df = accounts_df.filter(col("income") > 50000) \
    .select(lit("High Salary").alias("category")) \
    .groupBy("category").count()

# Combine all
result_df = low_df.unionByName(avg_df).unionByName(high_df)

OR

from pyspark.sql.functions import when, col

# Categorize income
df = accounts_df.withColumn(
    "category",
    when(col("income") < 20000, "Low Salary")
    .when((col("income") >= 20000) & (col("income") <= 50000), "Average Salary")
    .otherwise("High Salary")
)

# Group and count
result_df = df.groupBy("category").count().orderBy("category")

