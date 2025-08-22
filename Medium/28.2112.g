2112. The Airport With the Most Traffic
Description
Table: Flights

+-------------------+------+
| Column Name       | Type |
+-------------------+------+
| departure_airport | int  |
| arrival_airport   | int  |
| flights_count     | int  |
+-------------------+------+

(departure_airport, arrival_airport) is the primary key column for this table
Each row of this table indicates that there were flights_count flights
that departed from departure_airport and arrived at arrival_airport
 
Write an SQL query to report the ID of the airport with the most traffic
The airport with the most traffic is the airport that has the 
largest total number of flights that either departed from or 
arrived at the airport. If there is more than one airport with 
the most traffic, report them all

Return the result table in any order

The query result format is in the following example.

 

Example 1:

Input: 
Flights table:
+-------------------+-----------------+---------------+
| departure_airport | arrival_airport | flights_count |
+-------------------+-----------------+---------------+
| 1                 | 2               | 4             |
| 2                 | 1               | 5             |
| 2                 | 4               | 5             |
+-------------------+-----------------+---------------+
Output: 
+------------+
| airport_id |
+------------+
| 2          |
+------------+

Explanation: 
Airport 1 was engaged with 9 flights (4 departures, 5 arrivals).
Airport 2 was engaged with 14 flights (10 departures, 4 arrivals).
Airport 4 was engaged with 5 flights (5 arrivals).
The airport with the most traffic is airport 2.

Example 2:

Input: 
Flights table:
+-------------------+-----------------+---------------+
| departure_airport | arrival_airport | flights_count |
+-------------------+-----------------+---------------+
| 1                 | 2               | 4             |
| 2                 | 1               | 5             |
| 3                 | 4               | 5             |
| 4                 | 3               | 4             |
| 5                 | 6               | 7             |
+-------------------+-----------------+---------------+
Output: 
+------------+
| airport_id |
+------------+
| 1          |
| 2          |
| 3          |
| 4          |
+------------+

Explanation: 
Airport 1 was engaged with 9 flights (4 departures, 5 arrivals).
Airport 2 was engaged with 9 flights (5 departures, 4 arrivals).
Airport 3 was engaged with 9 flights (5 departures, 4 arrivals).
Airport 4 was engaged with 9 flights (4 departures, 5 arrivals).
Airport 5 was engaged with 7 flights (7 departures).
Airport 6 was engaged with 7 flights (7 arrivals).
The airports with the most traffic are airports 1, 2, 3, and 4.

---------------------
arrivals+dep so all traffic a+d needed, no a,d+d,a and using one col after union
---------------------
WITH traffic AS (
    SELECT departure_airport AS airport_id, SUM(flights_count) AS total
    FROM Flights
    GROUP BY departure_airport

    UNION ALL

    SELECT arrival_airport AS airport_id, SUM(flights_count) AS total
    FROM Flights
    GROUP BY arrival_airport
),
agg AS (
    SELECT airport_id, SUM(total) AS total_traffic
    FROM traffic
    GROUP BY airport_id
),
ranked AS (
    SELECT airport_id,
           RANK() OVER (ORDER BY total_traffic DESC) AS rnk
    FROM agg
)
SELECT airport_id
FROM ranked
WHERE rnk = 1;

or 

#instead of rank cte we can use 
WITH
    T AS (
        SELECT * FROM Flights
        UNION
        SELECT arrival_airport, departure_airport, flights_count FROM Flights
    ),
    P AS (
        SELECT departure_airport, sum(flights_count) AS cnt
        FROM T
        GROUP BY 1
    )
SELECT departure_airport AS airport_id
FROM P
WHERE cnt = (SELECT max(cnt) FROM P);

OR

SELECT airport_id
FROM (
    SELECT departure_airport AS airport_id, flights_count FROM Flights
    UNION ALL
    SELECT arrival_airport AS airport_id, flights_count FROM Flights
) AS combined
GROUP BY airport_id
HAVING SUM(flights_count) = (
    SELECT MAX(total_flights)
    FROM (
        SELECT airport_id, SUM(flights_count) AS total_flights
        FROM (
            SELECT departure_airport AS airport_id, flights_count FROM Flights
            UNION ALL
            SELECT arrival_airport AS airport_id, flights_count FROM Flights
        ) AS merged
        GROUP BY airport_id
    ) AS totals
);

---------------------------

from pyspark.sql.functions import col, sum as spark_sum

departure_df = flights_df.select(col("departure_airport").alias("airport_id"), "flights_count")
arrival_df = flights_df.select(col("arrival_airport").alias("airport_id"), "flights_count")
combined_df = departure_df.unionByName(arrival_df)
traffic_df = combined_df.groupBy("airport_id").agg(spark_sum("flights_count").alias("total_traffic"))
max_traffic = traffic_df.agg({"total_traffic": "max"}).collect()[0][0]
result_df = traffic_df.filter(col("total_traffic") == max_traffic)
result_df.select("airport_id").show()

or

from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as spark_sum, rank

combined_df = flights_df.selectExpr("departure_airport as airport_id", "flights_count") \
    .union(flights_df.selectExpr("arrival_airport as airport_id", "flights_count"))
agg_df = combined_df.groupBy("airport_id").agg(spark_sum("flights_count").alias("total_traffic"))
window_spec = Window.orderBy(col("total_traffic").desc())
ranked_df = agg_df.withColumn("rnk", rank().over(window_spec))
result_df = ranked_df.filter(col("rnk") == 1).select("airport_id")
result_df.show()

