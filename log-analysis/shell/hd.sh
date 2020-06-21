#! /bin/bash

case $1 in
	"start" )
		{
			echo " -------- 启动 hadoop集群 dfs和yarn -------"
			/home/sl/app/hadoop-2.7.2/sbin/start-dfs.sh
			/home/sl/app/hadoop-2.7.2/sbin/start-yarn.sh
		};;
	"stop" )
		{
			echo " -------- 停止 hadoop集群 dfs和yarn -------"
			/home/sl/app/hadoop-2.7.2/sbin/stop-yarn.sh
		    /home/sl/app/hadoop-2.7.2/sbin/stop-dfs.sh
		};;
esac