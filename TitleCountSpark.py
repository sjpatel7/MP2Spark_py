#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
import re

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
	#TODO
	stopWords = f.readlines()
for line in range(len(stopWords)):
    stopWords[line] = stopWords[line].rstrip()
stopWords.append("")
		
with open(delimitersPath) as f:
	#TODO
	delimiters = "[" + f.read() + "]"

conf = SparkConf().setMaster("local").setAppName("TitleCount") 
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

lines = sc.textFile(sys.argv[3],1)

#TODO
words = lines.flatMap(lambda line: re.split(delimiters, line)) #split lines and add delimited line to counts RDD
counts = words.filter(lambda word: word.lower() not in stopWords) \
		.map(lambda word: (word.lower(), 1)) \
		.reduceByKey(lambda a, b: a + b)
#swap key and val so that key is count and value is word. Then sort in descending order first
counts = counts.map(lambda w: (w[1], w[0])) \
		.sortByKey(ascending = False)

outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")
topTen = {}
for word in counts.take(10):
	topTen[word[1]] = str(word[0])

res = ""
for word in sorted(topTen):
	res = word + "\t" + topTen.get(word) + "\n" + res
outputFile.write(res)
outputFile.close()
sc.stop()
