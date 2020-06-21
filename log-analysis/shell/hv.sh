#! /bin/bash

case $1 in
	"start" )
		{
			echo " -------- 启动 hiveserver2 -------"
      nohup /home/sl/app/hive-2.3.6/bin/hive --service hiveserver2 > /home/sl/app/hive-2.3.6/nohup.out 2>@1 &
		};;
	"stop" )
		{
			echo " -------- 停止 hiveserver2 -------"
       ps -ef|grep hiveserver2 |grep -v grep | awk '{print $2 }' | xargs kill -9
		};;
esac