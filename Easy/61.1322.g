1322. 1322. Ads Performance
SQL Schema 
Table: Ads

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| ad_id         | int     |
| user_id       | int     |
| action        | enum    |
+---------------+---------+

(ad_id, user_id) is the primary key for this table.
Each row of this table contains the ID of an Ad, the ID of a user and the action taken by this user regarding this Ad.
The action column is an ENUM type of ('Clicked', 'Viewed', 'Ignored').

A company is running Ads and wants to calculate the performance of each Ad.

Performance of the Ad is measured using Click-Through Rate (CTR) where:

Write an SQL query to find the ctr of each Ad.

Round ctr to 2 decimal points. Order the result table by ctr in descending order and by ad_id in ascending order in case of a tie.

The query result format is in the following example:

Ads table:

+-------+---------+---------+
| ad_id | user_id | action  |
+-------+---------+---------+
| 1     | 1       | Clicked |
| 2     | 2       | Clicked |
| 3     | 3       | Viewed  |
| 5     | 5       | Ignored |
| 1     | 7       | Ignored |
| 2     | 7       | Viewed  |
| 3     | 5       | Clicked |
| 1     | 4       | Viewed  |
| 2     | 11      | Viewed  |
| 1     | 2       | Clicked |
+-------+---------+---------+

Result table:

+-------+-------+
| ad_id | ctr   |
+-------+-------+
| 1     | 66.67 |
| 3     | 50.00 |
| 2     | 33.33 |
| 5     | 0.00  |
+-------+-------+

for ad_id = 1, ctr = (2/(2+1)) * 100 = 66.67
for ad_id = 2, ctr = (1/(1+2)) * 100 = 33.33
for ad_id = 3, ctr = (1/(1+1)) * 100 = 50.00
for ad_id = 5, ctr = 0.00, Note that ad_id = 5 has no clicks or views.

Note that we don't care about Ignored Ads.
Result table is ordered by the ctr. in case of a tie we order them by ad_id


----------------------------------
WITH cte1 AS (
    SELECT ad_id,
           SUM(CASE WHEN action = 'Clicked' THEN 1 ELSE 0 END) AS clickcount,
           SUM(CASE WHEN action = 'Viewed' THEN 1 ELSE 0 END) AS viewcnt
    FROM Ads
    WHERE action IN ('Clicked', 'Viewed')  -- Ignored entries are excluded
    GROUP BY ad_id
)
SELECT ad_id,
       ROUND(clickcount * 100.0 / (clickcount + viewcnt), 2) AS ctr
FROM cte1
ORDER BY ctr DESC, ad_id ASC;

-------------------------
from pyspark.sql.functions import col, sum, when, lit, round

df = df1.filter(col("action").isin(["Clicked", "Viewed"])) \
    .groupBy("ad_id") \
    .agg(
        sum(when(col("action") == "Clicked", 1).otherwise(0)).alias("clickcount"),
        sum(when(col("action") == "Viewed", 1).otherwise(0)).alias("viewcount")
    ) \
    .select(
        col("ad_id"),
        round(col("clickcount") * 100.0 / (col("clickcount") + col("viewcount")), 2).alias("ctr")
    ) \
    .orderBy(col("ctr").desc(), col("ad_id").asc())


