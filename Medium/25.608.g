608. Tree Node
Given a table tree, id is identifier of the 
tree node and p_id is its parent node's id

+----+------+
| id | p_id |
+----+------+
| 1  | null |
| 2  | 1    |
| 3  | 1    |
| 4  | 2    |
| 5  | 2    |
+----+------+

Each node in the tree can be one of three types:
Leaf: if the node is a leaf node.
Root: if the node is the root of the tree.
Inner: If the node is neither a leaf node nor a root node.
Write a query to print the node id and the type of the node. 
Sort your output by the node id. The result for the above sample is:

+----+------+
| id | Type |
+----+------+
| 1  | Root |
| 2  | Inner|
| 3  | Leaf |
| 4  | Leaf |
| 5  | Leaf |
+----+------+

Explanation

Node '1' is root node, because its parent node is NULL and it has child node '2' and '3'.
Node '2' is inner node, because it has parent node '1' and child node '4' and '5'.
Node '3', '4' and '5' is Leaf node, because they have parent node and they don't have child node.

And here is the image of the sample tree as below:
 

			  1
			/   \
    2     3
         /   \
        4     5
Note

If there is only one node on the tree, you only need to output its root attributes.
-----------------------------
curent table se kya find krna chahte ho, agr child then current ko dusre table mai parent se dekho
uski child id milegi usse
-------------------------

SELECT 
    t1.id,
    CASE 
        WHEN t1.p_id IS NULL THEN 'Root'        -- No parent ⇒ root node
        WHEN t2.id IS NULL THEN 'Leaf'          -- No child match ⇒ leaf node
        ELSE 'Inner'                            -- Has both parent and child ⇒ inner node
    END AS Type
FROM tree t1
LEFT JOIN tree t2 ON t1.id = t2.p_id            -- If t1.id appears as parent in t2 ⇒ it's internal
GROUP BY t1.id, t1.p_id, t2.id                  -- Groups to remove duplicate rows caused by multiple children
ORDER BY t1.id;

OR

SELECT 
    id,
    CASE
        WHEN p_id IS NULL THEN 'Root'
        WHEN id NOT IN (SELECT DISTINCT p_id FROM tree WHERE p_id IS NOT NULL) THEN 'Leaf'
        ELSE 'Inner'
    END AS Type
FROM tree
ORDER BY id;

---------------------------
from pyspark.sql.functions import col, when, max

t1 = tree.alias("t1")
t2 = tree.alias("t2")

joined = t1.join(t2, col("t1.id") == col("t2.p_id"), "left")

result = (joined.groupBy("t1.id", "t1.p_id")
         .agg(max("t2.id").alias("has_child"))            # If at least one child exists
         .withColumn(
             "Type",
             when(col("t1.p_id").isNull(), "Root")        # No parent ⇒ root
             .when(col("has_child").isNull(), "Leaf")     # No child ⇒ leaf
             .otherwise("Inner")                          # Both parent and child ⇒ inner
         )
         .select(col("id"), "Type")
         .orderBy("id")
)

OR

from pyspark.sql.functions import when, col

# Find all parent node ids
parents = tree.select("p_id").where(col("p_id").isNotNull()).distinct()
parent_ids = [row["p_id"] for row in parents.collect()]

result = (
    tree.withColumn(
            "Type",
            when(col("p_id").isNull(), "Root")
            .when(~col("id").isin(parent_ids), "Leaf")
            .otherwise("Inner")
        )
        .select("id", "Type")
        .orderBy("id")
)

