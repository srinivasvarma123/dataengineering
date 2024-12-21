from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pyspark").getOrCreate()

sc = spark.sparkContext

from pyspark.sql.functions import *
data = [
    ('A', 'D', 'D'),
    ('B', 'A', 'A'),
    ('A', 'D', 'A')
]

df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")
df.show()

all_teams_df = df.select(
                        col("TeamA").alias("TeamName")).union(
                        df.select(col("TeamB").alias("TeamName"))
).distinct()
all_teams_df.show()

win_counts_df = (
                    df
                    .groupBy("Won")
                    .agg(count("Won").alias("WonCount"))
)

win_counts_df = win_counts_df.withColumnRenamed("Won", "TeamName")
win_counts_df.show()

result_df = (
    all_teams_df
    .join(win_counts_df, "TeamName", "left")
    .fillna({"WonCount": 0})
    .withColumnRenamed("WonCount", "Won")
    .orderBy("TeamName")
)
result_df.show()


