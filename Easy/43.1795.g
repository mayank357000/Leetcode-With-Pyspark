1795. Rearrange Products Table

Table: Products

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| product_id  | int     |
| store1      | int     |
| store2      | int     |
| store3      | int     |
+-------------+---------+
product_id is the primary key (column with unique values) for this table.
Each row in this table indicates the product's price in 3 different stores: 
store1, store2, and store3.
If the product is not available in a store, the price will be null in that store's column.
 

Write a solution to rearrange the Products table 
so that each row has (product_id, store, price). 
If a product is not available in a store, do not include a row with that 
product_id and store combination in the result table.

Return the result table in any order.

The result format is in the following example.

Example 1:

Input: 
Products table:
+------------+--------+--------+--------+
| product_id | store1 | store2 | store3 |
+------------+--------+--------+--------+
| 0          | 95     | 100    | 105    |
| 1          | 70     | null   | 80     |
+------------+--------+--------+--------+
Output: 
+------------+--------+-------+
| product_id | store  | price |
+------------+--------+-------+
| 0          | store1 | 95    |
| 0          | store2 | 100   |
| 0          | store3 | 105   |
| 1          | store1 | 70    |
| 1          | store3 | 80    |
+------------+--------+-------+

------------------------------------------------------
This type of transformation is classic in data wrangling:
 converting a wide format to a long format

----------------------------------------------------


with cte1 ascending(
    select product_id,'store1' as store,store1 as price where store1 is not null
    union
    select product_id,'store3' as store,store3 as price where store3 is not null
    union
    select product_id,'store2' as store,store2 as price where store2 is not null
)
select * from cte1;

-----------------------------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RearrangeProductsTable").getOrCreate()

# Sample data
data = [
    (0, 95, 100, 105),
    (1, 70, None, 80)
]
columns = ["product_id", "store1", "store2", "store3"]
df = spark.createDataFrame(data, columns)

# Unpivot using selectExpr and union (industry standard for small number of columns)
df1 = df.selectExpr("product_id", "'store1' as store", "store1 as price").filter("price is not null")
df2 = df.selectExpr("product_id", "'store2' as store", "store2 as price").filter("price is not null")
df3 = df.selectExpr("product_id", "'store3' as store", "store3 as price").filter("price is not null")

result_df = df1.union(df2).union(df3)

result_df.show()

----------------------------
df_store1 = df.withColumn("store", lit("store1")) \
    .withColumn("price", col("store1")) \
    .select("product_id", "store", "price") \
    .filter(col("price").isNotNull())

df_store2 = df.withColumn("store", lit("store2")) \
    .withColumn("price", col("store2")) \
    .select("product_id", "store", "price") \
    .filter(col("price").isNotNull())

df_store3 = df.withColumn("store", lit("store3")) \
    .withColumn("price", col("store3")) \
    .select("product_id", "store", "price") \
    .filter(col("price").isNotNull())

# Union all
result_df = df_store1.union(df_store2).union(df_store3)
--------------------------

from pyspark.sql import functions as F

# Sample DataFrame
df = spark.createDataFrame([
    (0, 95, 100, 105),
    (1, 70, None, 80)
], ["product_id", "store1", "store2", "store3"])

# Convert wide to long using array and explode
df_transformed = df.withColumn(
    "store_price_array",
    F.array(
        F.when(F.col("store1").isNotNull(), F.struct(F.lit("store1").alias("store"), F.col("store1").alias("price"))),
        F.when(F.col("store2").isNotNull(), F.struct(F.lit("store2").alias("store"), F.col("store2").alias("price"))),
        F.when(F.col("store3").isNotNull(), F.struct(F.lit("store3").alias("store"), F.col("store3").alias("price")))
    )
).select("product_id", F.explode("store_price_array").alias("exploded")) \
 .select("product_id", "exploded.store", "exploded.price")

df_transformed.show()