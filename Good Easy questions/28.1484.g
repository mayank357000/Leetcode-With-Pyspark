1484. Group Sold Products By The Date
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
