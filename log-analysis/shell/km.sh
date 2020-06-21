#! /bin/bash

case $1 in
	"start" )
		{
			echo "------------------- 启动 KafkaManager -----------------"
			nohup /home/sl/app/kafka-manager-1.3.3.7/bin/kafka-manager > kafka-manager-start.log 2>&1 &
		};;
	"stop"){
			echo "------------------- 停止 KafkaManager -----------------"
			ps -ef | grep ProdServerStart | grep -v grep |awk '{print $2}' |xargs kill
		};;
esac