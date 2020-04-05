#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
#count incoming links same way as part D
#then filter based on key in leagueIDS 
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

#each line has a league ID
leagueIds = sc.textFile(sys.argv[2], 1)

#TODO
leagueIds = leagueIds.collect()
#for i in range(len(leagueIds)):
#  leagueIds[i] = leagueIds[i].split()
leagueCounts = counts.filter(lambda p: p[0] in leagueIds) \
                      .map(lambda p: (p[1], p[0])) \
                      .sortByKey(ascending=True)

#w
pairs = leagueCounts.collect() #list of (count (int), id (str)) pairs in ascending count order
#calculate rank and then sort by id alphabetical order
ranks = {}
rankC = 0
for p in pairs:
  key = p[1]
  val = p[0]
  ranks[key] = rankC
  rankC += 1

output = open(sys.argv[3], "w")
#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
for key in sorted(ranks):
  output.write(str(key) + '\t' + str(ranks[key]) + '\n')
output.close()
sc.stop()

