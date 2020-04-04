#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1],1) #these are lines from output of partA

#TODO
N = 10
topN = lines.take(N)
sum = 0
mean = 0
min = -1
max = -1
var = 0
for line in topN:
  val = line.split("\t")[1]
  val = int(val)
  sum += val
  if min < 0:
    min = val
    max = val
  if val < min:
    min = val
  if val > max:
    max = val
try:
  mean = sum / len(topN)
  for line in topN:
    val = line.split("\t")[1]
    val = int(val)
    var += (val - mean) * (val - mean)
  var = var / len(topN)
except:
  print("size of TopTen is " + str(len(topN)))
  

outputFile = open(sys.argv[2],"w")

#TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % mean)
outputFile.write('Sum\t%s\n' % sum)
outputFile.write('Min\t%s\n' % min)
outputFile.write('Max\t%s\n' % max)
outputFile.write('Var\t%s\n' % var)


sc.stop()

