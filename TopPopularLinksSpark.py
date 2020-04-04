#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
def getCount(line):
  line = line.split(':')
  page = line[0]
  links = line[1].split(" ")
  for val in links:
    if not val.isdigit():
      links.remove(val)
  return (len(links), page)

counts = lines.map(lambda line: getCount(line)) \
              .sortByKey(ascending=False)

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
N = 10
topN = counts.take(N)
for count in counts:
  output.write(str(count[1]) + '\t' + str(count[0]) + '\n')
  
output.close()
sc.stop()

