#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:CycloneBoy
# datetime:2019/2/24 0:01
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def spark_analyse(filename):
    print("开始spark分析")
    # 程序主入口
    spark = SparkSession.builder.master("local").appName("rent_analyse").getOrCreate()
    df = spark.read.csv(filename, header=True)

    # max_list存储各个区的最大值，0海沧，1为湖里，2为集美，3为思明，4为翔安，5为同安;同理的mean_list, 以及min_list,approxQuantile中位数
    max_list = [0 for i in range(6)]
    mean_list = [1.2 for i in range(6)]
    min_list = [0 for i in range(6)]
    mid_list = [0 for i in range(6)]
    # 类型转换，十分重要，保证了price列作为int用来比较，否则会用str比较, 同时排除掉一些奇怪的价格，比如写字楼的出租超级贵
    # 或者有人故意标签1元，其实要面议, 还有排除价格标记为面议的
    df = df.filter(df.price != '面议').withColumn("price", df["price"].cast(IntegerType())). \
        filter(df.price >= 50).filter(df.price <= 40000)

    mean_list[0] = df.filter(df.area == "海沧").agg({"price": "mean"}).first()['avg(price)']
    mean_list[1] = df.filter(df.area == "湖里").agg({"price": "mean"}).first()['avg(price)']
    mean_list[2] = df.filter(df.area == "集美").agg({"price": "mean"}).first()['avg(price)']
    mean_list[3] = df.filter(df.area == "思明").agg({"price": "mean"}).first()['avg(price)']
    mean_list[4] = df.filter(df.area == "翔安").agg({"price": "mean"}).first()['avg(price)']
    mean_list[5] = df.filter(df.area == "同安").agg({"price": "mean"}).first()['avg(price)']

    min_list[0] = df.filter(df.area == "海沧").agg({"price": "min"}).first()['min(price)']
    min_list[1] = df.filter(df.area == "湖里").agg({"price": "min"}).first()['min(price)']
    min_list[2] = df.filter(df.area == "集美").agg({"price": "min"}).first()['min(price)']
    min_list[3] = df.filter(df.area == "思明").agg({"price": "min"}).first()['min(price)']
    min_list[4] = df.filter(df.area == "翔安").agg({"price": "min"}).first()['min(price)']
    min_list[5] = df.filter(df.area == "同安").agg({"price": "min"}).first()['min(price)']

    max_list[0] = df.filter(df.area == "海沧").agg({"price": "max"}).first()['max(price)']
    max_list[1] = df.filter(df.area == "湖里").agg({"price": "max"}).first()['max(price)']
    max_list[2] = df.filter(df.area == "集美").agg({"price": "max"}).first()['max(price)']
    max_list[3] = df.filter(df.area == "思明").agg({"price": "max"}).first()['max(price)']
    max_list[4] = df.filter(df.area == "翔安").agg({"price": "max"}).first()['max(price)']
    max_list[5] = df.filter(df.area == "同安").agg({"price": "max"}).first()['max(price)']

    # 返回值是一个list，所以在最后加一个[0]
    mid_list[0] = df.filter(df.area == "海沧").approxQuantile("price", [0.5], 0.01)[0]
    mid_list[1] = df.filter(df.area == "湖里").approxQuantile("price", [0.5], 0.01)[0]
    mid_list[2] = df.filter(df.area == "集美").approxQuantile("price", [0.5], 0.01)[0]
    mid_list[3] = df.filter(df.area == "思明").approxQuantile("price", [0.5], 0.01)[0]
    mid_list[4] = df.filter(df.area == "翔安").approxQuantile("price", [0.5], 0.01)[0]
    mid_list[5] = df.filter(df.area == "同安").approxQuantile("price", [0.5], 0.01)[0]

    all_list = []
    all_list.append(min_list)
    all_list.append(max_list)
    all_list.append(mean_list)
    all_list.append(mid_list)

    print("结束spark分析")

    return all_list