574. Winning Candidate
Table: Candidate

+-----+---------+
| id  | Name    |
+-----+---------+
| 1   | A       |
| 2   | B       |
| 3   | C       |
| 4   | D       |
| 5   | E       |
+-----+---------+
Table: Vote , votes each candidate got

+-----+--------------+
| id  | CandidateId  |
+-----+--------------+
| 1   |     2        |ye id 2 se match
| 2   |     4        |
| 3   |     3        |
| 4   |     2        |ye id 2 se match
| 5   |     5        |
+-----+--------------+
id is the auto-increment primary key,
CandidateId is the id appeared in Candidate table.

Write a sql to find the name of the winning candidate, the above example will return the winner B.

+------+
| Name |
+------+
| B    |
+------+
Notes:

You may assume there is no tie, in other words there will be at most one winning candidate.

-----------------------

SELECT c.Name
FROM Vote v
JOIN Candidate c ON v.CandidateId = c.id
GROUP BY c.id, c.Name
ORDER BY COUNT(*) DESC
LIMIT 1;

------------------------

from pyspark.sql import functions as F

vote_counts = (
    vote_df
    .groupBy("CandidateId")
    .agg(F.count("*").alias("num_votes"))
)

winning_candidate = (
    vote_counts
    .join(candidate_df, vote_counts.CandidateId == candidate_df.id)
    .orderBy(F.col("num_votes").desc())
    .limit(1)
    .select("Name")
)