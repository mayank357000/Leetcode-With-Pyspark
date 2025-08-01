1280. Students and Examinations

Table: Students

+---------------+---------+
| student_id    | int     |
| student_name  | varchar |
+---------------+---------+
student_id is the primary key (column with unique values) for this table.
Each row of this table contains the ID and the name of one student in the school.
 
Table: Subjects

+--------------+---------+
| subject_name | varchar |
+--------------+---------+
subject_name is the primary key (column with unique values) for this table.
Each row of this table contains the name of one subject in the school.

Table: Examinations

+--------------+---------+
| student_id   | int     |
| subject_name | varchar |
+--------------+---------+
There is no primary key (column with unique values) for this table. It may contain duplicates.
Each student from the Students table takes every course from the Subjects table.
Each row of this table indicates that a student with ID student_id attended the exam of subject_name.
 
Write a solution to find the number of times each student attended each exam.

Return the result table ordered by student_id and subject_name.

The result format is in the following example.

 

Example 1:

Input: 
Students table:
+------------+--------------+
| student_id | student_name |
+------------+--------------+
| 1          | Alice        |
| 2          | Bob          |
| 13         | John         |
| 6          | Alex         |
+------------+--------------+
Subjects table:
+--------------+
| subject_name |
+--------------+
| Math         |
| Physics      |
| Programming  |
+--------------+
Examinations table:
+------------+--------------+
| student_id | subject_name |
+------------+--------------+
| 1          | Math         |
| 1          | Physics      |
| 1          | Programming  |
| 2          | Programming  |
| 1          | Physics      |
| 1          | Math         |
| 13         | Math         |
| 13         | Programming  |
| 13         | Physics      |
| 2          | Math         |
| 1          | Math         |
+------------+--------------+
Output: 
+------------+--------------+--------------+----------------+
| student_id | student_name | subject_name | attended_exams |
+------------+--------------+--------------+----------------+
| 1          | Alice        | Math         | 3              |
| 1          | Alice        | Physics      | 2              |
| 1          | Alice        | Programming  | 1              |
| 2          | Bob          | Math         | 1              |
| 2          | Bob          | Physics      | 0              |
| 2          | Bob          | Programming  | 1              |
| 6          | Alex         | Math         | 0              |
| 6          | Alex         | Physics      | 0              |
| 6          | Alex         | Programming  | 0              |
| 13         | John         | Math         | 1              |
| 13         | John         | Physics      | 1              |
| 13         | John         | Programming  | 1              |
+------------+--------------+--------------+----------------+
Explanation: 
The result table should contain all students and all subjects.
Alice attended the Math exam 3 times, the Physics exam 2 times, and the Programming exam 1 time.
Bob attended the Math exam 1 time, the Programming exam 1 time, and did not attend the Physics exam.
Alex did not attend any exams.
John attended the Math exam 1 time, the Physics exam 1 time, and the Programming exam 1 time.


-----------------------------------------------

with cte1 as (
    select * from students cross join subjects
),
cte2 as (
    select student_id,subject_name, count(*) as count from examinations group by student_id,subject_name
)
select cte1.student_id,cte1.student_name,cte1.subject_name,ifnull(count,0) as attended_exams from
cte1 left join cte2 on cte1.student_id=cte2.student_id and cte1.subject_name=cte2.subject_name order by cte1.student_id,cte1.subject_name;

--------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, coalesce, lit

spark = SparkSession.builder.appName("StudentsAndExaminations").getOrCreate()

students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]
students_columns = ["student_id", "student_name"]

subjects_data = [
    ("Math",),
    ("Physics",),
    ("Programming",)
]
subjects_columns = ["subject_name"]

examinations_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]
examinations_columns = ["student_id", "subject_name"]

students_df = spark.createDataFrame(students_data, students_columns)
subjects_df = spark.createDataFrame(subjects_data, subjects_columns)
examinations_df = spark.createDataFrame(examinations_data, examinations_columns)

all_combinations_df = students_df.crossJoin(subjects_df)
#crossJoin has different syntax, no condition and no type/on and how

exam_counts_df = examinations_df.groupBy("student_id", "subject_name") \
    .agg(count("*").alias("attended_exams"))
#so we will have those roup by cols and agg cols in df

#can give a list of string,string,boolean in condition of equality
result_df = all_combinations_df.join(
    exam_counts_df,
    on=["student_id", "subject_name"],
    how="left"
).select(
    col("student_id"),
    col("student_name"),
    col("subject_name"),
    coalesce(col("attended_exams"), lit(0)).alias("attended_exams")
).orderBy("student_id", "subject_name")

result_df.show()
