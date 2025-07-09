1082. Sales Analysis I
Table: Product

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| product_id   | int     |
| product_name | varchar |
| unit_price   | int     |
+--------------+---------+
product_id is the primary key of this table

Table: Sales

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| seller_id   | int     |
| product_id  | int     |
| buyer_id    | int     |
| sale_date   | date    |
| quantity    | int     |
| price       | int     |
+------ ------+---------+
This table has no primary key, it can have repeated rows
product_id is a foreign key to Product table
 

Write an SQL query that reports the best seller by total sales price, If there is a tie, report them all.

The query result format is in the following example:

Product table:
+------------+--------------+------------+
| product_id | product_name | unit_price |
+------------+--------------+------------+
| 1          | S8           | 1000       |
| 2          | G4           | 800        |
| 3          | iPhone       | 1400       |
+------------+--------------+------------+

Sales table:
+-----------+------------+----------+------------+----------+-------+
| seller_id | product_id | buyer_id | sale_date  | quantity | price |
+-----------+------------+----------+------------+----------+-------+
| 1         | 1          | 1        | 2019-01-21 | 2        | 2000  |
| 1         | 2          | 2        | 2019-02-17 | 1        | 800   |
| 2         | 2          | 3        | 2019-06-02 | 1        | 800   |
| 3         | 3          | 4        | 2019-05-13 | 2        | 2800  |
+-----------+------------+----------+------------+----------+-------+

Result table:
+-------------+
| seller_id   |
+-------------+
| 1           |
| 3           |
+-------------+
Both sellers with id 1 and 3 sold products with the most total price of 2800.
------------------------------------------
when you have to take out max/min/count, and that single value has to be used
-----------------------------------------

with cte1 as(
    select seller_id,sum(price) as price 
    from sales
    group by 1
)
select seller_id from cte1 WHERE
price=(select max(price) from cte1)

----------------------------------------

from pyspark.sql import functions as F

# Step 1: Aggregate total price per seller
seller_sales = df.groupBy("seller_id") \
                 .agg(F.sum("price").alias("total_price"))

# Step 2: Find the maximum total price
max_price = seller_sales.agg(F.max("total_price").alias("max_price")).collect()[0]["max_price"]

#.collect() reutrns array of row objects, [Row(max_price=2800)]
#row object beahves a little like dictioanry and internally somehow allow use
#to access rowobject/array[idx].colname or array[idx]["col_name"]

#can do this too: max_price = result_df.first()["max_price"], behaves same as .collect()

# Step 3: Filter sellers matching the maximum price
top_sellers = seller_sales.filter(F.col("total_price") == max_price) \
                          .select("seller_id")