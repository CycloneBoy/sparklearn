#! /bin/bash

case $1 in
	"start" )
		{
			echo " -------- 启动 hbase -------"
			/home/sl/app/hbase-1.4.11/bin/start-hbase.sh
		};;
	"stop" )
		{
			echo " -------- 停止 hbase -------"
		  /home/sl/app/hbase-1.4.11/bin/stop-hbase.sh
		};;
esac