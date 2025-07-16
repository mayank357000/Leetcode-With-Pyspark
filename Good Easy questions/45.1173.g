1173. Immediate Food Delivery I
Best restaurants near me
Table: Delivery

+-----------------------------+---------+
| Column Name                 | Type    |
+-----------------------------+---------+
| delivery_id                 | int     |
| customer_id                 | int     |
| order_date                 | date    |
| customer_pref_delivery_date | date    |
+-----------------------------+---------+
delivery_id is the primary key of this table.
The table holds information about food delivery to customers that make orders at some date and specify a preferred delivery date (on the same order date or after it). 

If the preferred delivery date of the customer is the same as the order date then the order is called immediate otherwise it's called scheduled.

Write an SQL query to find the percentage of immediate orders in the table, rounded to 2 decimal places.

The query result format is in the following example:

Delivery table:
+-------------+-------------+------------+-----------------------------+
| delivery_id | customer_id | order_date | customer_pref_delivery_date |
+-------------+-------------+------------+-----------------------------+
| 1           | 1           | 2019-08-01 | 2019-08-02                  |
| 2           | 5           | 2019-08-02 | 2019-08-02                  |
| 3           | 1           | 2019-08-11 | 2019-08-11                  |
| 4           | 3           | 2019-08-24 | 2019-08-26                  |
| 5           | 4           | 2019-08-21 | 2019-08-22                  |
| 6           | 2           | 2019-08-11 | 2019-08-13                  |
+-------------+-------------+------------+-----------------------------+

Result table:
+----------------------+
| immediate_percentage |
+----------------------+
| 33.33                |
+----------------------+
------------------------------------------
SELECT 
  ROUND(SUM(order_date = customer_pref_delivery_date) / COUNT(*), 2) AS immediate_percentage 
FROM Delivery;
----------------------
this below is wrong
""""In PySpark, aggregation functions like sum(), count(), avg(), min(), max(), etc., 
are meant to work inside select and agg not with withColumn"""

df=df.withColumn("immediate_percentage",round(sum(when(order_date==customer_pref_delivery_date,1).otherwise(0)/count("*"),2))
above wrong below right 
df.agg(sum("column"), count("column"))
df.select(sum("column").alias("total_sum"))
--------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, sum, count, round, col

spark = SparkSession.builder.appName("solver").getOrCreate()

df = spark.read.csv("path/myfile.csv", header=True, inferSchema=True)
#Capital non string True in python

# Compute immediate and total counts
immediate_count = df.select(sum(when(col("order_date") == col("customer_pref_delivery_date"), 1).otherwise(0)).alias("immediate")).first()["immediate"]
total_count = df.count()

# Compute percentage
percentage = round(immediate_count / total_count * 100, 2)
print("Immediate Percentage:", percentage)
--------------------------------------------
easier way (sum(conditon) like sql won't work, expects col()objcect)

df=df.agg(round(sum(when(col("order_date")==col("customer_pref_delivery_date")).otherwise(0))/count("*")*100,2))
or
sum((col("order_date") == col("customer_pref_delivery_date")).cast("int"))

#cast retunrs a col() object only : 
#type((col("order_date") == col("customer_pref_delivery_date")).cast("int"))

--------------------------------


🧵 first()
- Returns: The first row of the DataFrame as a Row object.
- Use case: Extract a single row from a result like select() or agg() when you're expecting only one.
- ["colname"] to row obhect gives it value like dictonary
df.select(sum("sales").alias("total_sales")).first()["total_sales"]
, count() also gives single value
