from pyspark import SparkContext, SparkConf
from operator import add
import os
from urllib.parse import urlparse

os.environ["PYSPARK_PYTHON"] = "python3"
# conf = SparkConf().setAppName("study").setMaster("spark://10.104.157.113:7077")
conf = SparkConf().setAppName("study").setMaster("local")
sc = SparkContext(conf=conf)


# 返回url和count的元组
def url_count(line):
    url = line.split("\t")

    return url[1], 1


def get_subject(line):
    url = line[0]
    host = urlparse(url).netloc
    return (host, url, line[1])


def sort(iterator):
    result = []
    l = list(iterator)
    sort_list = sorted(l, key=lambda student: student[2], reverse=True)
    result.append(sort_list[0])
    result.append(sort_list[1])
    result.append(sort_list[2])
    return result


rdd = sc.textFile("/Users/willian/Desktop/project/python/spark/itcast.log").map(url_count)
rdd1 = rdd.reduceByKey(add)
rdd2 = rdd1.map(get_subject)
rdd3 = rdd2.groupBy(lambda tuple: tuple[0]).filter(lambda tuple:tuple[0]=="java.itcast.cn").mapValues(sort)
for url in rdd3.collect():
    print("url:{url},count:{count}".format(url=url[0], count=url[1]))
sc.stop()
