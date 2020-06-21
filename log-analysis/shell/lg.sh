#! /bin/bash

for i in  hadoop102 ; do
	ssh $i "java -classpath /home/sl/workspace/bigdata/appwarehouse/log-collector.jar com.cycloneboy.bigdata.loganalysis.logcollector.AppMain $1 $2 > /home/sl/workspace/bigdata/appwarehouse/test.log &"
done
