#! /bin/bash

case $1 in
	"start" ){

		echo " -------- 启动 zookeeper集群  -------"
		for i in hadoop102 hadoop103 hadoop104; do
			ssh $i  "/home/sl/app/zookeeper/bin/zkServer.sh start"
		done
		};;
	"stop" ){

		echo " -------- 停止 zookeeper集群  -------"
		for i in hadoop102 hadoop103 hadoop104; do
			ssh $i  "/home/sl/app/zookeeper/bin/zkServer.sh stop"
		done
		};;
	"status" ){

		echo " -------- 查看 zookeeper集群  -------"
		for i in hadoop102 hadoop103 hadoop104; do
			ssh $i  "/home/sl/app/zookeeper/bin/zkServer.sh status"
		done
		};;
esac