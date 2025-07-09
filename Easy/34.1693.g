1693. Daily Leads and Partners
Table: DailySales

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| date_id     | date    |
| make_name   | varchar |
| lead_id     | int     |
| partner_id  | int     |
+-------------+---------+
There is no primary key (column with unique values) for this table. It may contain duplicates.
This table contains the date and the name of the product sold and the IDs of the lead and partner it was sold to.
The name consists of only lowercase English letters.
 

For each date_id and make_name, find the number of distinct lead_id's and distinct partner_id's.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
DailySales table:
+-----------+-----------+---------+------------+
| date_id   | make_name | lead_id | partner_id |
+-----------+-----------+---------+------------+
| 2020-12-8 | toyota    | 0       | 1          |
| 2020-12-8 | toyota    | 1       | 0          |
| 2020-12-8 | toyota    | 1       | 2          |
| 2020-12-7 | toyota    | 0       | 2          |
| 2020-12-7 | toyota    | 0       | 1          |
| 2020-12-8 | honda     | 1       | 2          |
| 2020-12-8 | honda     | 2       | 1          |
| 2020-12-7 | honda     | 0       | 1          |
| 2020-12-7 | honda     | 1       | 2          |
| 2020-12-7 | honda     | 2       | 1          |
+-----------+-----------+---------+------------+
Output: 
+-----------+-----------+--------------+-----------------+
| date_id   | make_name | unique_leads | unique_partners |
+-----------+-----------+--------------+-----------------+
| 2020-12-8 | toyota    | 2            | 3               |
| 2020-12-7 | toyota    | 1            | 2               |
| 2020-12-8 | honda     | 2            | 2               |
| 2020-12-7 | honda     | 3            | 2               |
+-----------+-----------+--------------+-----------------+

--------------------------

SELECT
    date_id,
    make_name,
    COUNT(DISTINCT lead_id) AS unique_leads,
    COUNT(DISTINCT partner_id) AS unique_partners
FROM DailySales
GROUP BY date_id, make_name;\

---------------------------

result_df = dailysales_df.groupBy(["date_id", "make_name"]) \
    .agg(
        countDistinct("lead_id").alias("unique_leads"),
        countDistinct("partner_id").alias("unique_partners")
    ) \
    .select("date_id", "make_name", "unique_leads", "unique_partners")

----------------

if was asked to find the number of unique lead-partner pairs for each date_id:

SELECT
    date_id,
    COUNT(DISTINCT CONCAT(lead_id, '-', partner_id)) AS unique_lead_partner_pairs
FROM DailySales
GROUP BY date_id;

result_df = dailysales_df.groupBy("date_id") \
    .agg(countDistinct(concat(col("lead_id"), lit("-"), col("partner_id"))).alias("unique_lead_partner_pairs")) \
    .select("date_id", "unique_lead_partner_pairs")

or concat_ws("-", col("lead_id"), col("partner_id")) instead of concat if you want to use a separator:

-------------------

can use struct too in countDistinct(struct(lead_id, partner_id)) to count unique pairs:

Structs in PySpark act like Spark-native records or dictionaries—they 
let you group multiple columns together into a single column that holds a
 nested structure. While it’s not exactly a Python dict, it’s conceptually similar:
from pyspark.sql.functions import struct

df.withColumn("details", struct("lead_id", "partner_id"))

+--------+-----------+----------------+
|lead_id |partner_id |pair            |
+--------+-----------+----------------+
|1       |2          |{1, 2}          |
|3       |4          |{3, 4}          |

df.select("details.lead_id", "details.partner_id")
