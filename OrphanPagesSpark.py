#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 


#TODO
def getPages(line):
  line = line.rstrip()
  lines = line.split(":")
  p = lines[0].strip('\t\r\n\0 ')
  c = lines[1].strip('\t\r\n\0').split(" ")
  for val in c:
      if not val.isdigit():
          c.remove(val)
  res = [int(p)] + list(map(int, c))
  res[0] = -res[0]
  return res

def getVal(page):
  if page < 0:
    return (abs(page), 1) #possible orphan
  else:
    return (page, 0) #child
 
orphans = lines.flatMap(lambda line: getPages(line)) \
                .map(lambda p: getVal(p)) \
                .reduceByKey(lambda a, b: a * b) \
                .filter(lambda p: p[1] == 1) \
                .sortByKey(ascending = True)

output = open(sys.argv[2], "w")

for orphan in orphans.collect():
  output.write(str(orphan[0]) + "\n")
#TODO
#write results to output file. Foramt for each line: (line+"\n")

sc.stop()

