use gmall;

show tables;

-- dt = 2020-03-12
-- dt = 2020-03-13

select *
from ods_start_log
limit 10;

show partitions ods_start_log;

select *
from dwd_active_background_log
limit 10;

describe stud;

select *
from stud;

--------------------------------------------------------------
-- 系统函数
-- 2020-06-21
--------------------------------------------------------------
-- collect_set函数

-- 把同一分组的不同行的数据聚合成一个集合 
select course,
       collect_set(area),
       avg(score)
from stud
group by course;

select course,
       collect_list(area),
       avg(score)
from stud
group by course;

select course,
       collect_set(area)[0],
       avg(score)
from stud
group by course;

-- 日期函数
select date_format("2020-06-21", "yyyy-MM");
select date_add("2020-06-21", -1);
select date_add("2020-06-21", 1);

-- 星期一到星期日的英文（Monday，Tuesday、Wednesday、Thursday、Friday、Saturday、Sunday）

-- 取当前天的下一个周一
select next_day("2020-06-21", "MO");

-- 取当前周的周一
select date_add(next_day("2020-06-21", "MO"), -7);

-- last_day函数（求当月最后一天日期）
select last_day("2020-06-21");

--------------------------------------------------------------
--  需求一：用户活跃主题
-- 2020-06-21
--------------------------------------------------------------

-- DWS层
-- 目标：统计当日、当周、当月活动的每个设备明细

-- 每日活跃设备明细
drop table if exists dws_uv_detail_day;
create
external table dws_uv_detail_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度'
)
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_day';

show tables;

-- 以用户单日访问为key进行聚合，如果某个用户在一天中使用了两种操作系统、两个系统版本、
-- 多个地区，登录不同账号，只取其中之一

-- 导入数据
insert overwrite table dws_uv_detail_day
    partition (dt='2020-03-12')
select mid_id,
       concat_ws('|', collect_set(user_id))      user_id,
       concat_ws('|', collect_set(version_code)) version_code,
       concat_ws('|', collect_set(version_name)) version_name,
       concat_ws('|', collect_set(lang))         lang,
       concat_ws('|', collect_set(source))       source,
       concat_ws('|', collect_set(os))           os,
       concat_ws('|', collect_set(area))         area,
       concat_ws('|', collect_set(model))        model,
       concat_ws('|', collect_set(brand))        brand,
       concat_ws('|', collect_set(sdk_version))  sdk_version,
       concat_ws('|', collect_set(gmail))        gmail,
       concat_ws('|', collect_set(height_width)) height_width,
       concat_ws('|', collect_set(app_time))     app_time,
       concat_ws('|', collect_set(network))      network,
       concat_ws('|', collect_set(lng))          lng,
       concat_ws('|', collect_set(lat))          lat
from dwd_start_log
where dt = '2020-03-12'
group by mid_id;


select *
from dws_uv_detail_day
limit 5;

select count(*)
from dws_uv_detail_day;

-- 思考：不同渠道来源的每日活跃数统计怎么计算？

show partitions dwd_start_log;

select dt,
       count(*)
from dwd_start_log
group by dt;


-- 每周活跃设备明细
drop table if exists dws_uv_detail_wk;
create
external table dws_uv_detail_wk(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `monday_date` string COMMENT '周一日期',
    `sunday_date` string COMMENT  '周日日期'
) COMMENT '活跃用户按周明细'
PARTITIONED BY (`wk_dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_wk/'
;

-- 导入每周活跃用户
set hive.exec.dynamic.partition.mode = nonstrict;
insert overwrite table dws_uv_detail_wk partition (wk_dt)
select mid_id,
       concat_ws('|', collect_set(user_id))      user_id,
       concat_ws('|', collect_set(version_code)) version_code,
       concat_ws('|', collect_set(version_name)) version_name,
       concat_ws('|', collect_set(lang))         lang,
       concat_ws('|', collect_set(source))       source,
       concat_ws('|', collect_set(os))           os,
       concat_ws('|', collect_set(area))         area,
       concat_ws('|', collect_set(model))        model,
       concat_ws('|', collect_set(brand))        brand,
       concat_ws('|', collect_set(sdk_version))  sdk_version,
       concat_ws('|', collect_set(gmail))        gmail,
       concat_ws('|', collect_set(height_width)) height_width,
       concat_ws('|', collect_set(app_time))     app_time,
       concat_ws('|', collect_set(network))      network,
       concat_ws('|', collect_set(lng))          lng,
       concat_ws('|', collect_set(lat))          lat,
       date_add(next_day('2020-03-12', 'MO'), -7),
       date_add(next_day('2020-03-12', 'MO'), -1),
       concat(date_add(next_day('2020-03-12', 'MO'), -7), '_',
              date_add(next_day('2020-03-12', 'MO'), -1)
           )
from dws_uv_detail_day
where dt >= date_add(next_day('2020-03-12', 'MO'), -7)
  and dt <= date_add(next_day('2020-03-12', 'MO'), -1)
group by mid_id;

select *
from dws_uv_detail_wk
limit 5;
select count(*)
from dws_uv_detail_wk;

-- 每月活跃设备明细

drop table if exists dws_uv_detail_mn;
create
external table dws_uv_detail_mn(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度'
) COMMENT '活跃用户按月明细'
PARTITIONED BY (`mn` string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_mn/'
;

-- 数据导入

set hive.exec.dynamic.partition.mode = nonstrict;

insert overwrite table dws_uv_detail_mn partition (mn)
select mid_id,
       concat_ws('|', collect_set(user_id))      user_id,
       concat_ws('|', collect_set(version_code)) version_code,
       concat_ws('|', collect_set(version_name)) version_name,
       concat_ws('|', collect_set(lang))         lang,
       concat_ws('|', collect_set(source))       source,
       concat_ws('|', collect_set(os))           os,
       concat_ws('|', collect_set(area))         area,
       concat_ws('|', collect_set(model))        model,
       concat_ws('|', collect_set(brand))        brand,
       concat_ws('|', collect_set(sdk_version))  sdk_version,
       concat_ws('|', collect_set(gmail))        gmail,
       concat_ws('|', collect_set(height_width)) height_width,
       concat_ws('|', collect_set(app_time))     app_time,
       concat_ws('|', collect_set(network))      network,
       concat_ws('|', collect_set(lng))          lng,
       concat_ws('|', collect_set(lat))          lat,
       date_format('2020-03-12', 'yyyy-MM')
from dws_uv_detail_day
where date_format(dt, 'yyyy-MM') = date_format('2020-03-12', 'yyyy-MM')
group by mid_id;

select *
from dws_uv_detail_mn
limit 5;
select count(*)
from dws_uv_detail_mn;

-- 执行脚本导入下一天的
SELECT dt,
       count(*)
FROM dws_uv_detail_day
group by dt;

select wk_dt,
       count(*)
from dws_uv_detail_wk
group by wk_dt;

select mn,
       count(*)
from dws_uv_detail_mn
group by mn;


-- 活跃设备数
drop table if exists ads_uv_count;
create
external table ads_uv_count(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '每日活跃用户数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count_day/'
;


-- 导入数据
insert into table ads_uv_count
select '2020-03-12' dt,
       daycount.ct,
       wkcount.ct,
       mncount.ct,
       if(date_add(next_day('2020-03-12', 'MO'), -1) = '2020-03-12', 'Y', 'N'),
       if(last_day('2020-03-12') = '2020-03-12', 'Y', 'N')
from (
         select '2020-03-12' dt,
                count(*)     ct
         from dws_uv_detail_day
         where dt = '2020-03-12'
     ) daycount
         join
     (
         select '2020-03-12' dt,
                count(*)     ct
         from dws_uv_detail_wk
         where wk_dt = concat(date_add(next_day('2020-03-12', 'MO'), -7), '_',
                              date_add(next_day('2020-03-12', 'MO'), -1))
     ) wkcount on daycount.dt = wkcount.dt
         join
     (
         select '2020-03-12' dt,
                count(*)     ct
         from dws_uv_detail_mn
         where mn = date_format('2020-03-12', 'yyyy-MM')
     ) mncount on daycount.dt = mncount.dt;

-- 查询结果
select *
from ads_uv_count
limit 5;

select *
from ads_uv_count;
--------------------------------------------------------------
--  需求二：用户新增主题
-- 2020-06-21
--------------------------------------------------------------

-- DWS层（每日新增设备明细表）
drop table if exists dws_new_mid_day;
create
external table dws_new_mid_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `create_date`  string  comment '创建时间'
)  COMMENT '每日新增设备信息'
stored as parquet
location '/warehouse/gmall/dws/dws_new_mid_day/';

-- 导入数据
insert into table dws_new_mid_day
select ud.mid_id,
       ud.user_id,
       ud.version_code,
       ud.version_name,
       ud.lang,
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
       '2020-03-12'
from dws_uv_detail_day ud
         left join dws_new_mid_day nm on ud.mid_id = nm.mid_id
where ud.dt = '2020-03-12'
  and nm.mid_id is null;

select count(*)
from dws_new_mid_day;

-- ADS层（每日新增设备表）
drop table if exists ads_new_mid_count;
create
external  table  ads_new_mid_count
(
    `create_date`     string comment '创建时间',
    `new_mid_count`   BIGINT comment '新增设备数量'
)  COMMENT '每日新增设备信息数量'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';

-- 导入数据
insert into table ads_new_mid_count
select create_date,
       count(*)
from dws_new_mid_day
where create_date = '2020-03-12'
group by create_date;

select *
from ads_new_mid_count;

select *
from ads_new_mid_count;


--------------------------------------------------------------
--  需求三：用户留存主题
-- 2020-06-21
--------------------------------------------------------------

-- 留存用户：某段时间内的新增用户（活跃用户），经过一段时间后，又继续使用应用的被认作是留存用户；
-- 留存率：留存用户占当时新增用户（活跃用户）的比例即是留存率。

-- DWS层（每日留存用户明细表）

drop table if exists dws_user_retention_day;
create
external table dws_user_retention_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
   `create_date`    string  comment '设备新增时间',
   `retention_day`  int comment '截止当前日期留存天数'
)  COMMENT '每日用户留存情况'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_retention_day/'
;

-- 导入数据
insert overwrite table dws_user_retention_day partition (dt="2020-03-13")
select nm.mid_id,
       nm.user_id,
       nm.version_code,
       nm.version_name,
       nm.lang,
       nm.source,
       nm.os,
       nm.area,
       nm.model,
       nm.brand,
       nm.sdk_version,
       nm.gmail,
       nm.height_width,
       nm.app_time,
       nm.network,
       nm.lng,
       nm.lat,
       nm.create_date,
       1 retention_day
from dws_uv_detail_day ud
         join dws_new_mid_day nm on ud.mid_id = nm.mid_id
where ud.dt = '2020-03-12'
  and nm.create_date = date_add('2020-03-12', -1);

select count(*)
from dws_user_retention_day;

select *
from dws_user_retention_day;

-- 每天计算前1,2,3，n天的新用户访问留存明细
insert overwrite table dws_user_retention_day
    partition (dt="2020-03-12")
select nm.mid_id,
       nm.user_id,
       nm.version_code,
       nm.version_name,
       nm.lang,
       nm.source,
       nm.os,
       nm.area,
       nm.model,
       nm.brand,
       nm.sdk_version,
       nm.gmail,
       nm.height_width,
       nm.app_time,
       nm.network,
       nm.lng,
       nm.lat,
       nm.create_date,
       1 retention_day
from dws_uv_detail_day ud
         join dws_new_mid_day nm on ud.mid_id = nm.mid_id
where ud.dt = '2020-03-12'
  and nm.create_date = date_add('2020-03-12', -1)

union all
select nm.mid_id,
       nm.user_id,
       nm.version_code,
       nm.version_name,
       nm.lang,
       nm.source,
       nm.os,
       nm.area,
       nm.model,
       nm.brand,
       nm.sdk_version,
       nm.gmail,
       nm.height_width,
       nm.app_time,
       nm.network,
       nm.lng,
       nm.lat,
       nm.create_date,
       2 retention_day
from dws_uv_detail_day ud
         join dws_new_mid_day nm on ud.mid_id = nm.mid_id
where ud.dt = '2020-03-12'
  and nm.create_date = date_add('2020-03-12', -2)

union all
select nm.mid_id,
       nm.user_id,
       nm.version_code,
       nm.version_name,
       nm.lang,
       nm.source,
       nm.os,
       nm.area,
       nm.model,
       nm.brand,
       nm.sdk_version,
       nm.gmail,
       nm.height_width,
       nm.app_time,
       nm.network,
       nm.lng,
       nm.lat,
       nm.create_date,
       3 retention_day
from dws_uv_detail_day ud
         join dws_new_mid_day nm on ud.mid_id = nm.mid_id
where ud.dt = '2020-03-12'
  and nm.create_date = date_add('2020-03-12', -3);

-- 每天计算前1,2,3天的新用户访问留存明细
select retention_day, count(*)
from dws_user_retention_day
group by retention_day;

select *
from dws_user_retention_day;

-- 留存用户数
drop table if exists ads_user_retention_day_count;
create
external  table  ads_user_retention_day_count
(
    create_date string comment '设备新增日期',
    retention_day    int comment '截止当前日期留存天数',
    retention_count    bigint comment  '留存数量'
) COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_count/';

-- 导入数据
insert into table ads_user_retention_day_count
select create_date,
       retention_day,
       count(*) retention_count
from dws_user_retention_day
where dt = '2020-03-13'
group by create_date, retention_day;

select *
from ads_user_retention_day_count;

-- 留存用户比率
drop table if exists ads_user_retention_day_rate;
create
external table ads_user_retention_day_rate
(
       `stat_date`          string comment '统计日期',
       `create_date`        string  comment '设备新增日期',
       `retention_day`        int comment '截止当前日期留存天数',
       `retention_count`    bigint comment  '留存数量',
       `new_mid_count`        bigint comment '当日设备新增数量',
       `retention_ratio`        decimal(10,2) comment '留存率'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

insert into table ads_user_retention_day_rate
select '2020-03-13',
       ur.create_date,
       ur.retention_day,
       ur.retention_count,
       nc.new_mid_count,
       ur.retention_count / nc.new_mid_count * 100
from ads_user_retention_day_count ur
         join ads_new_mid_count nc
              on nc.create_date = ur.create_date;

select *
from ads_user_retention_day_rate;