#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
def getLinks(line):
  line = line.split(':')
  links = line[1].split(" ")
  for val in links:
    if not val.isdigit():
      links.remove(val)
  return links

counts = lines.flatMap(lambda line: getLinks(line)) \
              .map(lambda link: (link, 1)) \
              .reduceByKey(lambda a, b: a + b) \
              .map(lambda a: (a[1], a[0])) \
              .sortByKey(ascending=False)

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
N = 10
res = ""
for count in counts.take(N):
  res = str(count[1]) + '\t' + str(count[0]) + '\n' + res

output.write(res)
  
output.close()
sc.stop()

