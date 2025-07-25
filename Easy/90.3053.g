3053 - Classifying Triangles by Lengths
Posted on March 4, 2024
2 minute read
Welcome to Subscribe On Youtube

3053. Classifying Triangles by Lengths
Description
Table: Triangles

+-------------+------+ 
\| Column Name \| Type \| 
+-------------+------+ 
\| A           \| int  \| 
\| B           \| int  \|
\| C           \| int  \|
+-------------+------+
(A, B, C) is the primary key for this table.
Each row include the lengths of each of a triangle's three sides.

Write a query to find the type of triangle. Output one of the following for each row:

Equilateral: It's a triangle with 3 sides of equal length.
Isosceles: It's a triangle with 2 sides of equal length.
Scalene: It's a triangle with 3 sides of differing lengths.
Not A Triangle: The given values of A, B, and C don't form a triangle.
Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Triangles table:
+----+----+----+
\| A  \| B  \| C  \|
+----+----+----+
\| 20 \| 20 \| 23 \|
\| 20 \| 20 \| 20 \|
\| 20 \| 21 \| 22 \|
\| 13 \| 14 \| 30 \|
+----+----+----+
Output: 
+----------------+
\| triangle_type  \| 
+----------------+
\| Isosceles      \| 
\| Equilateral    \|
\| Scalene        \|
\| Not A Triangle \|
+----------------+
Explanation: 
- Values in the first row from an Isosceles triangle, because A = B.
- Values in the second row from an Equilateral triangle, because A = B = C.
- Values in the third row from an Scalene triangle, because A != B != C.
- Values in the fourth row cannot form a triangle, because the combined value of sides A and B is not larger than that of side C

---------------------------------
SELECT 
  CASE
    WHEN A + B <= C OR B + C <= A OR C + A <= B THEN 'Not A Triangle'
    WHEN A = B AND B = C THEN 'Equilateral'
    WHEN A = B OR B = C OR C = A THEN 'Isosceles'
    ELSE 'Scalene'
  END AS triangle_type
FROM Triangles;
----------------------------------
from pyspark.sql.functions import when, col

triangles = spark.createDataFrame([
    (20, 20, 23),
    (20, 20, 20),
    (20, 21, 22),
    (13, 14, 30)
], ["A", "B", "C"])

#WHEN KI CHAINING , ENDS WITH OThERHWISE

classified = triangles.withColumn(
    "triangle_type",
    when((col("A") + col("B") <= col("C")) |
         (col("B") + col("C") <= col("A")) |
         (col("C") + col("A") <= col("B")), "Not A Triangle")
    .when((col("A") == col("B")) & (col("B") == col("C")), "Equilateral")
    .when((col("A") == col("B")) | (col("B") == col("C")) | (col("C") == col("A")), "Isosceles")
    .otherwise("Scalene")
)

classified.select("triangle_type").show()