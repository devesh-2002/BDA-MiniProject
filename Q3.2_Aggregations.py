from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Aggregations") \
    .getOrCreate()

data = [(225,), (346,), (518,), (687,), (823,), (944,), (1056,),
        (1223,), (1375,), (1442,), (1565,), (1678,), (1790,), (1876,),
        (1943,)]

df = spark.createDataFrame(data, ["measurement"])

mean = df.agg(F.mean("measurement")).collect()[0][0]
sum_val = df.agg(F.sum("measurement")).collect()[0][0]
std_dev = df.agg(F.stddev("measurement")).collect()[0][0]
print("Output : ")
print(f"Mean is : {mean}\n Sum is {sum_val} \nStandard Deviation is {std_dev}" )
spark.stop()
