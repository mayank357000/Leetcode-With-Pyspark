2051. The Category of Each Member in the Store
Description
Table: Members

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| member_id   | int     |
| name        | varchar |
+-------------+---------+
member_id is the primary key column for this table.
Each row of this table indicates the name and the ID of a member.
 

Table: Visits

+-------------+------+
| Column Name | Type |
+-------------+------+
| visit_id    | int  |
| member_id   | int  |
| visit_date  | date |
+-------------+------+
visit_id is the primary key column for this table.
member_id is a foreign key to member_id from the Members table.
Each row of this table contains information about the date of a visit to the store and the member who visited it.

Table: Purchases

+----------------+------+
| Column Name    | Type |
+----------------+------+
| visit_id       | int  |
| charged_amount | int  |
+----------------+------+
visit_id is the primary key column for this table.
visit_id is a foreign key to visit_id from the Visits table.
Each row of this table contains information about the amount charged in a visit to the store.

A store wants to categorize its members. There are three tiers:

"Diamond": if the conversion rate is greater than or equal to 80.
"Gold": if the conversion rate is greater than or equal to 50 and less than 80.
"Silver": if the conversion rate is less than 50.
"Bronze": if the member never visited the store.
The conversion rate of a member is (100 * total number of purchases for the member) / total number of visits for the member.

Write an SQL query to report the id, the name, and the category of each member.

Return the result table in any order.

The query result format is in the following example.

Example 1:

Input: 
Members table:
+-----------+---------+
| member_id | name    |
+-----------+---------+
| 9         | Alice   |
| 11        | Bob     |
| 3         | Winston |
| 8         | Hercy   |
| 1         | Narihan |
+-----------+---------+
Visits table:
+----------+-----------+------------+
| visit_id | member_id | visit_date |
+----------+-----------+------------+
| 22       | 11        | 2021-10-28 |
| 16       | 11        | 2021-01-12 |
| 18       | 9         | 2021-12-10 |
| 19       | 3         | 2021-10-19 |
| 12       | 11        | 2021-03-01 |
| 17       | 8         | 2021-05-07 |
| 21       | 9         | 2021-05-12 |
+----------+-----------+------------+
Purchases table:
+----------+----------------+
| visit_id | charged_amount |
+----------+----------------+
| 12       | 2000           |
| 18       | 9000           |
| 17       | 7000           |
+----------+----------------+
Output: 
+-----------+---------+----------+
| member_id | name    | category |
+-----------+---------+----------+
| 1         | Narihan | Bronze   |
| 3         | Winston | Silver   |
| 8         | Hercy   | Diamond  |
| 9         | Alice   | Gold     |
| 11        | Bob     | Silver   |
+-----------+---------+----------+
Explanation: 
- User Narihan with id = 1 did not make any visits to the store. She gets a Bronze category.
- User Winston with id = 3 visited the store one time and did not purchase anything. The conversion rate = (100 * 0) / 1 = 0. He gets a Silver category.
- User Hercy with id = 8 visited the store one time and purchased one time. The conversion rate = (100 * 1) / 1 = 1. He gets a Diamond category.
- User Alice with id = 9 visited the store two times and purchased one time. The conversion rate = (100 * 1) / 2 = 50. She gets a Gold category.
- User Bob with id = 11 visited the store three times and purchased one time. The conversion rate = (100 * 1) / 3 = 33.33. He gets a Silver category.

-------------------
great use count(col) check not null values count in col, but count(*) not null records count
-------------------

WITH conversion_rate_cte AS (
    SELECT 
        v.member_id,
        SUM(CASE WHEN p.visit_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS conversion_rate
    FROM Visits v
    LEFT JOIN Purchases p ON v.visit_id = p.visit_id
    GROUP BY v.member_id
)

SELECT 
    m.member_id,
    m.name,
    CASE 
        WHEN cr.member_id IS NULL THEN 'Bronze'
        WHEN cr.conversion_rate >= 80 THEN 'Diamond'
        WHEN cr.conversion_rate >= 50 THEN 'Gold'
        ELSE 'Silver'
    END AS category
FROM Members m
LEFT JOIN conversion_rate_cte cr ON m.member_id = cr.member_id

or

WITH visit_stats AS (
    SELECT v.member_id,
           COUNT(*) AS total_visits,
           COUNT(p.visit_id) AS total_purchases
    FROM Visits v
    LEFT JOIN Purchases p ON v.visit_id = p.visit_id
    GROUP BY v.member_id
)

SELECT m.member_id,
       m.name,
       CASE
           WHEN vs.member_id IS NULL THEN 'Bronze'
           WHEN (100.0 * vs.total_purchases / vs.total_visits) >= 80 THEN 'Diamond'
           WHEN (100.0 * vs.total_purchases / vs.total_visits) >= 50 THEN 'Gold'
           ELSE 'Silver'
       END AS category
FROM Members m
LEFT JOIN visit_stats vs ON m.member_id = vs.member_id;

--------------------------------

from pyspark.sql import functions as F

# Visit stats: total visits and purchases per member
visit_stats = (
    visits_df.alias("v")
    .join(purchases_df.alias("p"), F.col("v.visit_id") == F.col("p.visit_id"), "left")
    .groupBy("v.member_id")
    .agg(
        F.count("v.visit_id").alias("total_visits"),
        F.count("p.visit_id").alias("total_purchases")
    )
)

# Members with categories
result_df = (
    members_df.alias("m")
    .join(visit_stats.alias("vs"), "member_id", "left")
    .withColumn(
        "category",
        F.when(F.col("vs.member_id").isNull(), "Bronze")
         .when((F.col("total_purchases") * 100.0 / F.col("total_visits")) >= 80, "Diamond")
         .when((F.col("total_purchases") * 100.0 / F.col("total_visits")) >= 50, "Gold")
         .otherwise("Silver")
    )
    .select("member_id", "name", "category")
)

or

from pyspark.sql import functions as F

conversion_rate_cte = (
    visits_df.alias("v")
    .join(purchases_df.alias("p"), F.col("v.visit_id") == F.col("p.visit_id"), "left")
    .groupBy("v.member_id")
    .agg(
        (F.sum(F.when(F.col("p.visit_id").isNotNull(), 1).otherwise(0)) * 100.0 / F.count("v.visit_id")).alias("conversion_rate")
    )
)

result_df = (
    members_df.alias("m")
    .join(conversion_rate_cte.alias("cr"), "member_id", "left")
    .withColumn(
        "category",
        F.when(F.col("cr.member_id").isNull(), "Bronze")
         .when(F.col("conversion_rate") >= 80, "Diamond")
         .when(F.col("conversion_rate") >= 50, "Gold")
         .otherwise("Silver")
    )
    .select("member_id", "name", "category")
)