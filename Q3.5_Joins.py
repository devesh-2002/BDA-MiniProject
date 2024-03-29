from pyspark import SparkContext

sc = SparkContext("local", "Joins")

left_data = sc.parallelize([(101, "John"), (102, "Alice"), (103, "Bob"), (104, "Emily")])
right_data = sc.parallelize([(101, 25), (102, 30), (105, 28)])

map_join = left_data.join(right_data)

reduce_join = left_data.union(right_data).reduceByKey(lambda x, y: (x, y))

print("Map Side Join:")
for result in map_join.collect():
    print(result)

print("\nReduce Side Join:")
for result in reduce_join.collect():
    print(result)

sc.stop()
