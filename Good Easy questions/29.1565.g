1565. Unique Orders and Customers Per Month
Description
Table: Orders

+---------------+---------+
| order_id      | int     |
| order_date    | date    |
| customer_id   | int     |
| invoice       | int     |
+---------------+---------+
order_id is the primary key for this table.
This table contains information about the orders made by customer_id.
 

Write an SQL query to find the number of unique orders and the number of unique customers with invoices > $20 for each different month.

Return the result table sorted in any order.

The query result format is in the following example.

 

Example 1:

Input: 
Orders table:
+----------+------------+-------------+------------+
| order_id | order_date | customer_id | invoice    |
+----------+------------+-------------+------------+
| 1        | 2020-09-15 | 1           | 30         |
| 2        | 2020-09-17 | 2           | 90         |
| 3        | 2020-10-06 | 3           | 20         |
| 4        | 2020-10-20 | 3           | 21         |
| 5        | 2020-11-10 | 1           | 10         |
| 6        | 2020-11-21 | 2           | 15         |
| 7        | 2020-12-01 | 4           | 55         |
| 8        | 2020-12-03 | 4           | 77         |
| 9        | 2021-01-07 | 3           | 31         |
| 10       | 2021-01-15 | 2           | 20         |
+----------+------------+-------------+------------+
Output: 
+---------+-------------+----------------+
| month   | order_count | customer_count |
+---------+-------------+----------------+
| 2020-09 | 2           | 2              |
| 2020-10 | 1           | 1              |
| 2020-12 | 2           | 1              |
| 2021-01 | 1           | 1              |
+---------+-------------+----------------+

-----------------------------------------------------

SELECT
    SUBSTRING(order_date, 1, 7) AS month,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT CASE WHEN invoice > 20 THEN customer_id END) AS customer_count
FROM Orders
GROUP BY SUBSTRING(order_date, 1, 7);

-- here count(distinct case when returning col) help us with customer_count
-- sum(condition)=number of rows not unique rows, could not help us here hence used count
-- count does not count null

<!-- In SQL, when you use CASE WHEN ... THEN ... END without an ELSE, the default is ELSE NULL.
So, for rows where invoice > 20 is not true, the expression returns NULL.
COUNT(DISTINCT ...) ignores NULLsit only counts non-null, distinct values. -->

<!-- SELECT
    CONCAT(YEAR(order_date), '-', MONTH(order_date), 2, '0')) AS month,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT CASE WHEN invoice > 20 THEN customer_id END) AS customer_count
FROM Orders
GROUP BY CONCAT(YEAR(order_date), '-', LPAD(MONTH(order_date), 2, '0')); -->

------------
SELECT
  DATE_FORMAT(order_date, '%Y-%m') AS month,
  COUNT(DISTINCT order_id) AS order_count,
  COUNT(DISTINCT CASE WHEN invoice > 20 THEN customer_id END) AS customer_count
FROM Orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m');
-----------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, substring, when

spark = SparkSession.builder.appName("UniqueOrdersAndCustomersPerMonth").getOrCreate()

# Sample data
data = [
    (1, "2020-09-15", 1, 30),
    (2, "2020-09-17", 2, 90),
    (3, "2020-10-06", 3, 20),
    (4, "2020-10-20", 3, 21),
    (5, "2020-11-10", 1, 10),
    (6, "2020-11-21", 2, 15),
    (7, "2020-12-01", 4, 55),
    (8, "2020-12-03", 4, 77),
    (9, "2021-01-07", 3, 31),
    (10, "2021-01-15", 2, 20),
]
columns = ["order_id", "order_date", "customer_id", "invoice"]

df = spark.createDataFrame(data, columns)

# Extract month as 'YYYY-MM'
df = df.withColumn("month", substring("order_date", 1, 7))

# Group by month and aggregate
result_df = df.groupBy("month").agg(
    countDistinct("order_id").alias("order_count"),
    countDistinct(when(col("invoice") > 20, col("customer_id"))).alias("customer_count")
)

result_df.show()
