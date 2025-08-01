1623. All Valid Triplets That Can Represent a Country

Table: SchoolA

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| student_id    | int     |
| student_name  | varchar |
+---------------+---------+
student_id is the primary key for this table.
Each row of this table contains the name and the id of a student in school A.
All student_name are distinct.
 

Table: SchoolB

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| student_id    | int     |
| student_name  | varchar |
+---------------+---------+
student_id is the primary key for this table.
Each row of this table contains the name and the id of a student in school B.
All student_name are distinct.
 

Table: SchoolC

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| student_id    | int     |
| student_name  | varchar |
+---------------+---------+
student_id is the primary key for this table.
Each row of this table contains the name and the id of a student in school C.
All student_name are distinct.
 

There is a country with three schools, where each student is enrolled in exactly one school. The country is joining a competition and wants to select one student from each school to represent the country such that:

member_A is selected from SchoolA,
member_B is selected from SchoolB,
member_C is selected from SchoolC, and
The selected students' names and IDs are pairwise distinct 
(i.e. no two students share the same name, and no two students share the same ID).
Write an SQL query to find all the possible triplets 
representing the country under the given constraints.

Return the result table in any order.

The query result format is in the following example.

SchoolA table:
+------------+--------------+
| student_id | student_name |
+------------+--------------+
| 1          | Alice        |
| 2          | Bob          |
+------------+--------------+

SchoolB table:
+------------+--------------+
| student_id | student_name |
+------------+--------------+
| 3          | Tom          |
+------------+--------------+

SchoolC table:
+------------+--------------+
| student_id | student_name |
+------------+--------------+
| 3          | Tom          |
| 2          | Jerry        |
| 10         | Alice        |
+------------+--------------+

Result table:
+----------+----------+----------+
| member_A | member_B | member_C |
+----------+----------+----------+
| Alice    | Tom      | Jerry    |
| Bob      | Tom      | Alice    |
+----------+----------+----------+

Let us see all the possible triplets.
- (Alice, Tom, Tom) --> Rejected because member_B and member_C have the same name and the same ID.
- (Alice, Tom, Jerry) --> Valid triplet.
- (Alice, Tom, Alice) --> Rejected because member_A and member_C have the same name.
- (Bob, Tom, Tom) --> Rejected because member_B and member_C have the same name and the same ID.
- (Bob, Tom, Jerry) --> Rejected because member_A and member_C have the same ID.
- (Bob, Tom, Alice) --> Valid triplet.

-----------------------------------------

SELECT 
    a.student_name AS member_A,
    b.student_name AS member_B,
    c.student_name AS member_C
FROM 
    SchoolA a
JOIN 
    SchoolB b ON a.student_id != b.student_id AND a.student_name != b.student_name
JOIN 
    SchoolC c ON a.student_id != c.student_id AND b.student_id != c.student_id
           AND a.student_name != c.student_name AND b.student_name != c.student_name;

------------------------------------------

from pyspark.sql.functions import col

# Assuming dfA, dfB, dfC are DataFrames for SchoolA, SchoolB, and SchoolC
dfA = spark.table("SchoolA")
dfB = spark.table("SchoolB")
dfC = spark.table("SchoolC")

# Join A with B using inequality conditions
joinedAB = dfA.alias("a").join(
    dfB.alias("b"),
    (col("a.student_id") != col("b.student_id")) & 
    (col("a.student_name") != col("b.student_name"))
)

# Join the above with C, again applying all distinctness constraints
final_df = joinedAB.join(
    dfC.alias("c"),
    (col("a.student_id") != col("c.student_id")) &
    (col("b.student_id") != col("c.student_id")) &
    (col("a.student_name") != col("c.student_name")) &
    (col("b.student_name") != col("c.student_name"))
)

# Select the required columns
result = final_df.select(
    col("a.student_name").alias("member_A"),
    col("b.student_name").alias("member_B"),
    col("c.student_name").alias("member_C")
)

---------------
here we can use alias still in next df transformation step 
