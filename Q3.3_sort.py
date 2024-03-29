from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SortingExample") \
    .getOrCreate()

data = [
    (101, "John", 50000),
    (102, "Alice", 60000),
    (103, "Bob", 45000),
    (104, "Emily", 70000),
    (105, "Michael", 55000),
    (106, "Emma", 62000),
    (107, "David", 48000)
]

df = spark.createDataFrame(data, ["emp_id", "emp_name", "salary"])

print("Before Sorting:")
df.show()

sorted_df = df.orderBy("salary")

print("After Sorting:")
sorted_df.show()

spark.stop()
