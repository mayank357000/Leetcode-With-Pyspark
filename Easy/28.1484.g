SELECT
    sell_date,
    COUNT(DISTINCT product) AS num_sold,
    GROUP_CONCAT(DISTINCT product ORDER BY product ASC) AS products
FROM Activities
GROUP BY sell_date
ORDER BY sell_date;

---------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, collect_set, array_sort, concat_ws

spark = SparkSession.builder.appName("Activities").getOrCreate()

# Sample data
data = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Bible"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-shirt"),
]
columns = ["sell_date", "product"]

df = spark.createDataFrame(data, columns)

-- #concat_withseparator
result_df = df.groupBy("sell_date") \
    .agg(
        countDistinct("product").alias("num_sold"),
        concat_ws(",", array_sort(collect_set("product"))).alias("products")
    ) \
    .orderBy("sell_date")

result_df.show(truncate=False)


------------
here collect_set returns a "spark_array col()", so it is also a col
array_sort is used to sort spark_array col and reutrns a spark array col() obj, so still col()
and concat_ws inside agg(all 3 inside agg) return string, can take input of multiple col() objects, a spark array col
concat_ws(" | ", lit("User"), col("id"), col("role"))


from pyspark.sql.functions import collect_set, array_sort

df.groupBy("sell_date").agg(
    array_sort(collect_set("product")).alias("sorted_products")
).show()

sorted_products
["apple", "banana"]
----------------------------

Spark array col ke methods hai dono

Yes, **`array_sort`** and **`collect_set`** are PySpark SQL functions.

### How do they work in a 'groupBy' aggregation?
we are using spark array col methods here, one makes spark array col
and other is used for sorting

- **`collect_set("product")`**: 
 When used in .groupBy().agg(collect_set("column")), it returns a Spark array column (not a Python list). 
  For each group (here, each `sell_date`), this function collects all unique values of the `"product"` column into a Python list (actually, a Spark array type).  
  Example: For `2020-05-30`, it returns `["Headphone", "Basketball", "T-shirt"]`.

- **`array_sort(...)`**: 
array_sort is a Spark SQL function that takes a Spark array column as input, not a Python list. 
  This function sorts the array (list) in ascending (lexicographical) order.  
  Example: `array_sort(["Headphone", "Basketball", "T-shirt"])` → `["Basketball", "Headphone", "T-shirt"]`.

- **`concat_ws(",", ...)`**:  
  This function joins the elements of the array into a single string, separated by commas.  
  Example: `concat_ws(",", ["Basketball", "Headphone", "T-shirt"])` → `"Basketball,Headphone,T-shirt"`.

-- ### Why can you use them this way?

-- - In PySpark, when you use `.groupBy("sell_date").agg(...)`, the aggregation functions (`countDistinct`, `collect_set`(return array), etc.) operate **on each group**.
-- - You pass the column name as a string (e.g., `"product"`), and PySpark applies the function to that column **within each group**.
-- - The functions are chained: `collect_set` creates an array, `array_sort` sorts it, and `concat_ws` turns it into a string.

+----------+-------------------------------+
|sell_date |collect_set(product)           |
+----------+-------------------------------+
|2020-05-30|[Headphone, Basketball, T-shirt]|
+----------+-------------------------------+

-------------

Yes, using Spark array columns is a common and powerful pattern in PySpark, especially for aggregation, transformation, and complex data processing.

Why are Spark array columns useful?
They allow you to aggregate multiple values into a single row (e.g., all products sold on a date).
You can then apply further transformations (sort, filter, join, explode, etc.) on these arrays efficiently in a distributed way.
Common Spark Array Column Functions
Besides collect_set and array_sort, here are other frequently used array functions in PySpark:

collect_list(col): Like collect_set, but keeps duplicates.

size(array_col): Returns the length of the array.

explode(array_col): Flattens the array so each element becomes a separate row.

array_contains(array_col, value): Checks if an array contains a value.

sort_array(array_col): Alias for array_sort.

concat(array_col1, array_col2, ...): Concatenates arrays.
flatten(array_col): Flattens nested arrays into a single array.
slice(array_col, start, length): Returns a slice of the array.
reverse(array_col): Reverses the order of elements in the array.
array_distinct(array_col): Removes duplicate values from the array.


-------------------

You rarely store Spark array columns as a final result in a database.
Usually, you use array columns as an intermediate step (for grouping, sorting, deduplication, etc.).
At the end, you typically convert them to a string (using concat_ws) or explode them back to rows before writing to a database or producing a report.