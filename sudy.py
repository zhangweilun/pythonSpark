# conf = SparkConf().setAppName("study")
# sc = SparkContext(conf=conf)
# def filterOutFromPartion(list):
#     # iterator = []
#     for elements in list:
#         yield [x for  x in elements if x !=2 ]
#         # iterator.append([x for  x in elements if x !=2 ])
#     # return iter(iterator)
#
# data = [[1,2,3],[3,2,4],[5,2,7]]
#
# partitions = sc.parallelize(data, 2).mapPartitions(filterOutFromPartion).collect()
# print(partitions)

# def add(a, b):
#     "Same as a + b."
#     return a + b
#
# def getone(x):
#     return (x,1)
#
# conf = SparkConf().setAppName("WC")
# spark_context = SparkContext(conf=conf)
# result = spark_context.textFile("/Users/willian/Public/spark/README.md").flatMap(lambda line: line.split(" ")).map(getone).reduceByKey(add).collect()
# for (word, count) in result:
#     print("%s: %i" % (word, count))