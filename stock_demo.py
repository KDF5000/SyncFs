# -*- coding: utf-8 -*-
# Spark Application - execute with spark-submit

# imports
from pyspark import SparkConf, SparkContext

# Module Constants
APP_NAME = "My Spark Application"


# Main Functionality
def main(sc):
    url = "/user/hadoop/data"
    rdd_mkt_data = sc.wholeTextFiles(url, minPartitions=80) \
                     .setName("index_minute_bar") \
                     .cache()
    # 指定要预测的线id<这里预测2016.03.17的分钟线
    target_line = "000001.ZICN-20160317"
    # 指定用于计算县四度的分钟线长度
    # minute_bar_length = 90  # 9：30 - 11：00
    # minute_bar_length_share = sc.broadcast(minute_bar_length)
    line_data = rdd_mkt_data.filter(lambda x: target_line in x[0]).take(1)
    print "Result",line_data
    # target_line_share = sc.broadcast(target_line_mkt_data)


if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("spark://172.16.48.108:7077")
    sc = SparkContext(conf=conf)

    # Execute Main
    main(sc)
