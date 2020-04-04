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
  line = line.split(":")
  line = [int(line[0])] + list(map(int, line[1].split(" ")))
  line[0] = -line[0]
  return line

def page_kv(line):
  pair = line.split(": ")
  key = int(pair[0])
  val = list(map(int, pair[1].split(" ")))
  return (key, val)

def getVal(page):
  if page < 0:
    return (abs(page), 1) #possible orphan
  else:
    return (page, 0) #child
  
orphans = lines.flatMap(lambda line: getPages(line)) \
                .map(lambda p: getVal(p)) \
                 #0 if child, 1 if orphan
                .reduceByKey(lambda a, b: a * b) \ 
                .filter(lambda p: p[1] == 1) \
                .sortByKey(ascending = True)

output = open(sys.argv[2], "w")

for orphan in orphans.collect():
  output.write(orphan[0] + "\n")
#TODO
#write results to output file. Foramt for each line: (line+"\n")

sc.stop()

