#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:CycloneBoy
# datetime:2019/2/23 23:58


from fishrent import draw, rent_analyse

if __name__ == '__main__':
    print("开始总程序")
    Filename = "rent.csv"
    # rentspider.run()
    all_list = rent_analyse.spark_analyse(Filename)
    draw.draw_bar(all_list)
    print("结束总程序")