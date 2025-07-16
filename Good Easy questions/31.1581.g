1581. Customer Who Visited but Did Not Make Any Transactions
Table: Visits

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| visit_id    | int     |
| customer_id | int     |
+-------------+---------+
visit_id is the column with unique values for this table.
This table contains information about the customers who visited the mall.
 

Table: Transactions

+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| transaction_id | int     |
| visit_id       | int     |
| amount         | int     |
+----------------+---------+
transaction_id is column with unique values for this table.
This table contains information about the transactions made during the visit_id.
 

Write a solution to find the IDs of the users who visited without making any transactions and the number of times they made these types of visits.

Return the result table sorted in any order.

The result format is in the following example.

 

Example 1:

Input: 
Visits
+----------+-------------+
| visit_id | customer_id |
+----------+-------------+
| 1        | 23          |
| 2        | 9           |
| 4        | 30          |
| 5        | 54          |
| 6        | 96          |
| 7        | 54          |
| 8        | 54          |
+----------+-------------+
Transactions
+----------------+----------+--------+
| transaction_id | visit_id | amount |
+----------------+----------+--------+
| 2              | 5        | 310    |
| 3              | 5        | 300    |
| 9              | 5        | 200    |
| 12             | 1        | 910    |
| 13             | 2        | 970    |
+----------------+----------+--------+
Output: 
+-------------+----------------+
| customer_id | count_no_trans |
+-------------+----------------+
| 54          | 2              |
| 30          | 1              |
| 96          | 1              |
+-------------+----------------+
------------------------------------

select customer_id,count(*) as count_no_trans
from visits v left join transactions t
on v.visit_id=t.visit_id
where transaction_id is null
group by customer_id;

------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("NoTransactionVisits").getOrCreate()

visits_data = [
    (1, 23),
    (2, 9),
    (4, 30),
    (5, 54),
    (6, 96),
    (7, 54),
    (8, 54)
]
visits_columns = ["visit_id", "customer_id"]

transactions_data = [
    (2, 5, 310),
    (3, 5, 300),
    (9, 5, 200),
    (12, 1, 910),
    (13, 2, 970)
]
transactions_columns = ["transaction_id", "visit_id", "amount"]

visits_df = spark.createDataFrame(visits_data, visits_columns)
transactions_df = spark.createDataFrame(transactions_data, transactions_columns)

result_df = visits_df.join(
    transactions_df,
    visits_df.visit_id == transactions_df.visit_id,
    how="left"
).filter(
    col("transaction_id").isNull()
).groupBy(
    "customer_id"
).agg(
    count("*").alias("count_no_trans")
)

result_df.show()
