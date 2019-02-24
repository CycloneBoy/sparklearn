#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:CycloneBoy
# datetime:2019/2/24 0:57

from pyspark.context import SparkContext
import jieba
# from pyspark.sql.session import SparkSession
# from pyspark.ml import Pipeline
# from pyspark.ml.feature import StringIndexer, VectorIndexer


def run():
    sc = SparkContext("local", "WordCount")   #初始化配置
    data = sc.textFile(r"D:\python\data\小王子.txt")   #读取是utf-8编码的文件
    with open(r'D:\python\data\stopwords-master\百度停用词表.txt','r',encoding='utf-8') as f:
        x=f.readlines()
    stop=[i.replace('\n','') for i in x]
    stop.extend(['，','的','我','他','','。',' ','\n','？','；','：','-','（','）','！','1909','1920','325','B612','II','III','IV','V','VI','—','‘','’','“','”','…','、'])#停用标点之类
    data=data.flatMap(lambda line: jieba.cut(line,cut_all=False)).filter(lambda w: w not in stop).\
        map(lambda w:(w,1)).reduceByKey(lambda w0,w1:w0+w1).sortBy(lambda x:x[1],ascending=False)
    print(data.take(100))

if __name__ == '__main__':
    run()
