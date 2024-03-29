from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("MatrixMultiplicationExample") \
    .getOrCreate()

matrix_A_data = [(1, 2, 3),
                 (4, 5, 6),
                 (7, 8, 9)]

matrix_B_data = [(9, 8, 7),
                 (6, 5, 4),
                 (3, 2, 1)]

matrix_A_df = spark.createDataFrame(matrix_A_data, ["A1", "A2", "A3"])
matrix_B_df = spark.createDataFrame(matrix_B_data, ["B1", "B2", "B3"])

result_matrix = matrix_A_df.crossJoin(matrix_B_df) \
    .withColumn("result", sum(F.col("A{0}".format(i + 1)) * F.col("B{0}".format(j + 1))
                              for i in range(3) for j in range(3))) \
    .select("result") \
    .rdd.zipWithIndex() \
    .map(lambda x: (x[1] // 3, x[1] % 3, x[0][0])) \
    .toDF(["row", "col", "value"]) \
    .groupBy("row").pivot("col").agg(F.first("value"))

result_matrix.show()

spark.stop()
