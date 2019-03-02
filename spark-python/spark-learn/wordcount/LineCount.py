#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:CycloneBoy
# datetime:2019/2/24 11:45

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('LineCount')
    sc = SparkContext(conf=conf)
    filename_hdfs = "hdfs://localhost:9000/sparklearn/data/小王子.txt"
    filename_linux = "file:///media/sl/D/java/idea/a2019/sparklearn/data/小王子.txt"
    filename_win = "D:\\java\\idea\\a2019\\sparklearn\\data\\小王子.txt"
    lines = sc.textFile(filename_linux)
    print(lines.count())

