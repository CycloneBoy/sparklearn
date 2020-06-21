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
 insert into table "$APP".dws_new_mid_day
select
    ud.mid_id,
    ud.user_id ,
    ud.version_code ,
    ud.version_name ,
    ud.lang ,
    ud.source,
    ud.os,
    ud.area,
    ud.model,
    ud.brand,
    ud.sdk_version,
    ud.gmail,
    ud.height_width,
    ud.app_time,
    ud.network,
    ud.lng,
    ud.lat,
    '$do_date'
from "$APP".dws_uv_detail_day ud left join "$APP".dws_new_mid_day nm on ud.mid_id=nm.mid_id
where ud.dt='$do_date' and nm.mid_id is null;
"

$hive -e "$sql"