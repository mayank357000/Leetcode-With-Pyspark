1440. Evaluate Boolean Expression
Table Variables:

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| name          | varchar |
| value         | int     |
+---------------+---------+
name is the primary key for this table.
This table contains the stored variables and their values.

Table Expressions:

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| left_operand  | varchar |
| operator      | enum    |
| right_operand | varchar |
+---------------+---------+
(left_operand, operator, right_operand) 
is the primary key for this table.
This table contains a boolean expression 
that should be evaluated.
operator is an enum that takes one 
of the values ('<', '>', '=')
The values of left_operand and right_operand are 
guaranteed to be in the Variables table.

Write an SQL query to evaluate the boolean 
expressions in Expressions table.

Return the result table in any order.

The query result format is in the following example.

Variables table:
+------+-------+
| name | value |
+------+-------+
| x    | 66    |
| y    | 77    |
+------+-------+

Expressions table:
+--------------+----------+---------------+
| left_operand | operator | right_operand |
+--------------+----------+---------------+
| x            | >        | y             |
| x            | <        | y             |
| x            | =        | y             |
| y            | >        | x             |
| y            | <        | x             |
| x            | =        | x             |
+--------------+----------+---------------+

Result table:
+--------------+----------+---------------+-------+
| left_operand | operator | right_operand | value |
+--------------+----------+---------------+-------+
| x            | >        | y             | false |
| x            | <        | y             | true  |
| x            | =        | y             | false |
| y            | >        | x             | true  |
| y            | <        | x             | false |
| x            | =        | x             | true  |
+--------------+----------+---------------+-------+
As shown, you need find the value of each boolean 
exprssion in the table using the variables table.

----------------------------------
we need two join here, one will give x its value and other y its value
-----------------------------------
SELECT 
    e.left_operand,
    e.operator,
    e.right_operand,
    CASE 
        WHEN e.operator = '>' THEN lv.value > rv.value
        WHEN e.operator = '<' THEN lv.value < rv.value
        WHEN e.operator = '=' THEN lv.value = rv.value
    END AS value
FROM 
    Expressions e
join 
    Variables lv on e.left_operand=lv.name
join 
    Variables rv on e.right_operand=rv.name;

--------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

variables_data = [("x", 66), ("y", 77)]
expressions_data = [
    ("x", ">", "y"),
    ("x", "<", "y"),
    ("x", "=", "y"),
    ("y", ">", "x"),
    ("y", "<", "x"),
    ("x", "=", "x")
]

spark = SparkSession.builder.getOrCreate()

variables_df = spark.createDataFrame(variables_data, ["name", "value"])
expressions_df = spark.createDataFrame(expressions_data, ["left_operand", "operator", "right_operand"])

joined_df = expressions_df \
    .join(variables_df.withColumnRenamed("name", "left_operand")\
    .withColumnRenamed("value", "left_value"), on="left_operand") \
    .join(variables_df.withColumnRenamed("name", "right_operand")\
    .withColumnRenamed("value", "right_value"), on="right_operand")

result_df = joined_df.withColumn(
    "value",
    when(col("operator") == ">", col("left_value") > col("right_value"))
    .when(col("operator") == "<", col("left_value") < col("right_value"))
    .when(col("operator") == "=", col("left_value") == col("right_value"))
)

final_df = result_df.select("left_operand", "operator", "right_operand", "value")

final_df.show()