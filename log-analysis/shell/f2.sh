#! /bin/bash

case $1
	in
	"start" ){
	for i in hadoop102 ; do
		echo " -------------- 启动 $i 消费flume -----------------"
		ssh $i "nohup /home/sl/app/flume/bin/flume-ng agent --conf-file /home/sl/app/flume/job/kafka-flume-hdfs.conf --name a1 -Dflume.root.logger=INFO,LOGFILE > /home/sl/app/flume/log.txt 2>&1 &"
	done	
	};;
	"stop"){
	for i in hadoop102	; do
		echo " -------------- 停止 $i 消费flume -----------------"
		ssh $i "ps -ef |grep kafka-flume-hdfs | grep -v grep |awk '{print \$2}' | xargs kill"
	done

	};;
esac