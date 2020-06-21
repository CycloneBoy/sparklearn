#! /bin/bash

case $1 in
	"start" )
		{
			for i in hadoop102 ; do
				echo "----------------- 启动 $i kafka---------------"
				ssh $i "/home/sl/app/kafka/bin/kafka-server-start.sh -daemon /home/sl/app/kafka/config/server.properties "
			done
		};;
	"stop")
		{
			for i in hadoop102 ; do
				echo "----------------- 停止 $i kafka---------------"
				ssh $i "/home/sl/app/kafka/bin/kafka-server-stop.sh"
			done
		};;
esac