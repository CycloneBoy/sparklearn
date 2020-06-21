#! /bin/bash

case $1 in
	"start" )
		{
			echo " -------- 启动 集群 -------"

			#启动 Hadoop集群
			/home/sl/bin/hd.sh start

			#启动 Zookeeper集群
			/home/sl/bin/zk.sh start

			sleep 4s;

			#启动 Flume采集集群
			/home/sl/bin/f1.sh start

			#启动 Kafka采集集群
			/home/sl/bin/kf.sh start

			sleep 6s;

			#启动 Flume消费集群
			/home/sl/bin/f2.sh start

			#启动 KafkaManager
			/home/sl/bin/km.sh start
		};;
	"stop" )
		{
			echo " -------- 停止 集群 -------"

			#停止 KafkaManager
			/home/sl/bin/km.sh stop

		    #停止 Flume消费集群
			/home/sl/bin/f2.sh stop

			#停止 Kafka采集集群
			/home/sl/bin/kf.sh stop

		    sleep 6s;

			#停止 Flume采集集群
			/home/sl/bin/f1.sh stop

			#停止 Zookeeper集群
			/home/sl/bin/zk.sh stop

			#停止 Hadoop集群
			/home/sl/bin/hd.sh stop
			
		};;

esac