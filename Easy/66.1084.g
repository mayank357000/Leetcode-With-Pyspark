1084. Sales Analysis III
Table: Product

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| product_id   | int     |
| product_name | varchar |
| unit_price   | int     |
+--------------+---------+
product_id is the primary key of this table.

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
This table has no primary key, it can have repeated rows.
product_id is a foreign key to Product table.
 

Write an SQL query that reports the products that were only sold in spring 2019. That is, between 2019-01-01 and 2019-03-31 inclusive.

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
+-------------+--------------+
| product_id  | product_name |
+-------------+--------------+
| 1           | S8           |
+-------------+--------------+
The product with id 1 was only sold in spring 2019 while the other two were sold after.

------------------------------------------------

SELECT p.product_id, p.product_name
FROM Product p
WHERE product_id IN (
    SELECT product_id
    FROM Sales
    GROUP BY product_id
    HAVING MIN(sale_date) >= '2019-01-01'
       AND MAX(sale_date) <= '2019-03-31'
)

OR

SELECT DISTINCT p.product_id, p.product_name
FROM Product p
JOIN Sales s ON p.product_id = s.product_id
WHERE s.sale_date BETWEEN '2019-01-01' AND '2019-03-31'
  AND NOT EXISTS (
      SELECT 1
      FROM Sales s2
      WHERE s2.product_id = p.product_id
        AND (s2.sale_date < '2019-01-01' OR s2.sale_date > '2019-03-31')
  )
---------------

from pyspark.sql.functions import col, min, max

# Filter products whose all sale_dates are between 2019-01-01 and 2019-03-31
filtered_sales = sales_df.groupBy("product_id") \
    .agg(
        min("sale_date").alias("min_date"),
        max("sale_date").alias("max_date")
    ) \
    .filter((col("min_date") >= "2019-01-01") & (col("max_date") <= "2019-03-31"))

# Join with product table to get product_name
result_df = filtered_sales.join(product_df, "product_id") \
    .select("product_id", "product_name")

OR

similar to non exist logic, use anti join

# Sales outside spring 2019
outside_sales = sales_df.filter(
    (col("sale_date") < "2019-01-01") | (col("sale_date") > "2019-03-31")
).select("product_id").distinct()

# Products sold in spring 2019
spring_sales = sales_df.filter(
    (col("sale_date") >= "2019-01-01") & (col("sale_date") <= "2019-03-31")
).select("product_id").distinct()

# Anti-join to exclude those with sales outside spring 2019
exclusive_spring_sales = spring_sales.join(outside_sales, "product_id", "anti")

# Join with product table
result_df = exclusive_spring_sales.join(product_df, "product_id") \
    .select("product_id", "product_name")