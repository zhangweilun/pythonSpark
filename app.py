from pyspark import SparkContext, SparkConf

import os

os.environ["PYSPARK_PYTHON"] = "python3"


def add(a, b):
    "Same as a + b."
    return a + b


conf = SparkConf().setAppName("WC")
spark_context = SparkContext(conf=conf)
result = spark_context.textFile("/Users/willian/Public/spark/README.md").flatMap(lambda line: line.split(" ")).map(
    lambda x: (x, 1)).reduceByKey(add).collect()
for (word, count) in result:
    print("%s: %i" % (word, count))

spark_context.stop()
