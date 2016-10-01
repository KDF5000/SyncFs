
#-*- coding: utf-8 -*-
## Spark Application - execute with spark-submit

##imports
from pyspark import SparkConf, SparkContext

##Module Constants
APP_NAME = "My Spark Application"

##Cosure Functions

## Main Functionality

def main(sc):
    logFile = "/user/hadoop/README.md"
    logData = sc.textFile(logFile).cache()
    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()
    print "Lines with a: %i, lines with b: %i"%(numAs, numBs)


if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("spark://172.16.48.108:7077")
    sc = SparkContext(conf=conf)

    # Execute Main
    main(sc)