from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Search") \
    .getOrCreate()

data = [("Apple", "iPhone 13"), ("Samsung", "Galaxy S21"), ("Google", "Pixel 6"), 
        ("Huawei", "Mate 40"), ("Xiaomi", "Mi 11"), ("OnePlus", "9 Pro")]

df = spark.createDataFrame(data, ["brand", "model"])

search_result = df.filter(df.brand == "Samsung").collect()
if search_result:
    print("Found:", search_result[0])
else:
    print("Not Found")

search_result = df.filter(df.brand == "Sony").collect()
if search_result:
    print("Found:", search_result[0])
else:
    print("Not Found")

spark.stop()
