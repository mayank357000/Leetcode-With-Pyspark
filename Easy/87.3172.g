3172. Second Day Verification 🔒
Description
Table: emails

+-+-+
\| email_id    \| int      \|
\| user_id     \| int      \|
\| signup_date \| datetime \|
+-++++-++
\| email_id \| user_id \| signup_date         \|
++-++
texts table:

+-+--+++
\| 1       \| 125      \| Verified     \| 2022-06-15 08:30:00\|
\| 2       \| 433      \| Not Verified \| 2022-07-10 10:45:00\|
\| 4       \| 234      \| Verified     \| 2022-08-21 09:30:00\|
+-+--++
\| user_id \|
++
Explanation:

User with email_id 7005 signed up on 2022-08-20 10:00:00 and verified on second day of the signup.
User with email_id 7771 signed up on 2022-06-14 09:30:00 and verified on second day of the signup.

------------------
SELECT e.email_id
FROM emails e
JOIN texts t
  ON e.user_id = t.user_id
  AND t.action = 'Verified'
  AND DATEDIFF(t.action_date, e.signup_date) = 1;
------------------------
from pyspark.sql.functions import col, datediff

verified_texts = texts_df.filter(col("action") == "Verified")

joined_df = emails_df.join(verified_texts, on="user_id")

second_day_verified = joined_df.filter(datediff(col("action_date"), col("signup_date")) == 1)

result_df = second_day_verified.select("email_id")