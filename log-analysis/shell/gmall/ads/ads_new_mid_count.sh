#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=$HIVE_HOME/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else
	do_date=`date -d "-1 day" +%F`
fi

sql="
  insert into table "$APP".ads_new_mid_count
select
create_date,
count(*)
from "$APP".dws_new_mid_day
where create_date='$do_date'
group by create_date;
"

$hive -e "$sql"