#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:CycloneBoy
# datetime:2019/2/24 11:45

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('LineCount')
    sc = SparkContext(conf=conf)
    lines = sc.textFile("C:\\app\\spark-2.4.0-bin-hadoop2.7\\README.md")
    print(lines.count())