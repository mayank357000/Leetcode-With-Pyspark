3198. Find Cities in Each State 🔒
Description
Table: cities

+-++
\| state       \| varchar \|
\| city        \| varchar \|
+-++
\| state       \| city          \|
+-++
Output:

+-++
\| California  \| Los Angeles, San Diego, San Francisco \|
\| New York    \| Buffalo, New York City, Rochester     \|
\| Texas       \| Austin, Dallas, Houston               \|
+-+---+
Explanation:

California: All cities ("Los Angeles", "San Diego", "San Francisco") are listed in a comma-separated string.
New York: All cities ("Buffalo", "New York City", "Rochester") are listed in a comma-separated string.
Texas: All cities ("Austin", "Dallas", "Houston") are listed in a comma-separated string.

Note: The output table is ordered by the state name in ascending order.
--------------------------
SELECT 
  state,
  GROUP_CONCAT(city ORDER BY city SEPARATOR ', ') AS city_list
FROM cities
GROUP BY state
ORDER BY state;
--------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, array_sort, concat_ws

# Start SparkSession
spark = SparkSession.builder.getOrCreate()

# Sample DataFrame (replace with actual source)
cities_df = spark.createDataFrame([
    ("California", "San Francisco"),
    ("California", "Los Angeles"),
    ("California", "San Diego"),
    ("New York", "New York City"),
    ("New York", "Buffalo"),
    ("New York", "Rochester"),
    ("Texas", "Dallas"),
    ("Texas", "Austin"),
    ("Texas", "Houston")
], ["state", "city"])

# 🔡 Group cities per state into comma-separated string
result_df = cities_df.groupBy("state") \
    .agg(
        concat_ws(", ", array_sort(collect_list("city"))).alias("city_list")
    ) \
    .orderBy("state")

# 💬 Show result
result_df.show(truncate=False)