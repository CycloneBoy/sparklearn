-- 2020-06-13
create database learnhive comment "learn hive";

use learnhive;

show tables;
-------------------------------------------------------------------
--   第四章 HiveSql 数据定义
-------------------------------------------------------------------
create table if not exists learnhive.employees
(
    name string,
    salary float,
    subordinates array< STRING >,
    deductions map< STRING,
    float>,
    address struct<street: STRING,
    city: STRING,
    state: STRING,
    zip: int >
)
    comment 'test table'
    tblproperties
    ('creator'='sl','create_at'='2020-06-13');

show tables;

show create table learnhive.employees;

show tblproperties learnhive.employees;

create table if not exists learnhive.employees2
    like learnhive.employees;

show tables;
show tables like 'empl.*';

describe extended learnhive.employees;
describe formatted learnhive.employees;

create
external table if not exists stocks(
    `exchange` string,
    symbol string,
    ymd string,
    price_open float,
    price_high float,
    price_low float,
    price_close float,
    volume int,
    price_adj_close float
)
row format delimited fields terminated by ','
location '/data/stocks';

show tables;

describe extended learnhive.stocks;

drop table learnhive.employees;

create table if not exists learnhive.employees
(
    name string,
    salary float,
    subordinates array< STRING >,
    deductions map< STRING,
    float>,
    address struct<street: STRING,
    city: STRING,
    state: STRING,
    zip: int >
) partitioned by (country string,state string)
tblproperties ('creator'='sl','create_at'='2020-06-13');

alter table learnhive.employees
    add partition (country='US',state='AL');
alter table learnhive.employees
    add partition (country='US',state='CA');

set hive.mapred.mod = strict;

select *
from learnhive.employees;

describe extended learnhive.employees;

show partitions learnhive.employees;

create table if not exists learnhive.employees
(
    name string,
    salary float,
    subordinates array< STRING >,
    deductions map< STRING,
    float>,
    address struct<street: STRING,
    city: STRING,
    state: STRING,
    zip: int >
) partitioned by (country string,state string)
row format delimited fields terminated by '\001'
    collection items terminated by '\002'
    map keys terminated by '\003'
    lines terminated by '\n'
    stored as textfile
tblproperties ('creator'='sl','create_at'='2020-06-13');

load data local inpath '/home/sl/learnhive/prog-hive-1st-ed-data/data/employees/employees.txt'
into table learnhive.employees
partition (country='US',state='CA');

select *
from learnhive.employees;

select *
from learnhive.employees
where country = 'US'
  AND state = 'CA';

select name, address.city, address.zip
from learnhive.employees;

describe extended learnhive.employees;

describe formatted learnhive.employees;

show tables;


drop table learnhive.stocks;

create
external table if not exists stocks(
    `exchange` string,
    symbol string,
    ymd string,
    price_open float,
    price_high float,
    price_low float,
    price_close float,
    volume int,
    price_adj_close float
)
clustered by (`exchange`,symbol)
    sorted by (ymd asc )
    into 96 buckets
row format delimited fields terminated by ','
location '/data/stocks';

describe formatted learnhive.stocks;

create
external table if not exists log_messages(
    hms int,
    severity string,
    server string,
    process_id int,
    message string
)
partitioned by (year int,month int , day int)
row format delimited fields terminated by '\t';

alter table log_messages
    add partition (year =2020,month=6,day=13) location 'hdfs://localhost:9000/data/log_messages/2020/6/13';

alter table log_messages
    add if not exists
    partition (year =2020, month =6, day =13) location 'hdfs://localhost:9000/data/log_messages/2020/6/13'
    partition (year =2020, month =6, day =14) location 'hdfs://localhost:9000/data/log_messages/2020/6/14';

alter table log_messages
    drop if exists  partition (year=2020,month=6,day=13);

alter table log_messages
    change column hms hours_minutes_seconds int
        comment 'the hour minutes seconds '
        after severity;

alter table log_messages
    add columns (
    app_name string comment 'app name',
    session_id string comment 'the current session id'
    );

describe formatted learnhive.log_messages;

alter table log_messages replace columns (
    hms int comment 'the hour minutes seconds',
    severity string comment 'the message servreity',
    server string comment 'server'
    );

show create table log_messages;

alter table learnhive.log_messages set tblproperties (
    'notes' = 'the process id '
    );

alter table log_messages partition (year =2020,month=6,day=14)
set fileformat sequencefile;

alter table table_using_json_storage set serde 'com.example.JsonSerDe'
    with serdeproperties (
    'prop1'='value1',
    'prop2'='value2'
    );

alter table table_using_json_storage set serdeproperties (
    'prop3'='value3',
    'prop4'='value4'
    );

alter table learnhive.stocks
    clustered by (`exchange`,symbol)
    sorted by (symbol)
    into 48 buckets;

alter table log_messages touch partition (year =2020, month =6, day =14);

-- alter table log_messages archive partition (year=2020,month=6,day=14);
alter table log_messages partition (year =2020,month=6,day=14) enable no_drop;


-------------------------------------------------------------------
--   第五章 HiveSql 数据操作
-------------------------------------------------------------------
show tables;

from employees2 se
insert into table learnhive.employees
    partition (country='US', state='OR')
select *
where se.country = 'US'
  and se.state = 'OR'
insert into table learnhive.employees
    partition (country='US', state='CA')
select *
where se.country = 'US'
  and se.state = 'CA'
insert into table learnhive.employees
    partition (country='US', state='IL')
select *
where se.country = 'US'
  and se.state = 'IL';

-- 动态分区插入
insert overwrite table learnhive.employees
    partition (country, state)
select se.name, se.salary, se.deductions, se.subordinates
from learnhive.employees2 se;

insert overwrite table learnhive.employees
    partition (country='US', state)
select se.name, se.salary, se.deductions, se.subordinates
from learnhive.employees2 se
where se.country = 'US';

-- 动态分区属性

-- 注意，动态分区不允许主分区采用动态列而副分区采用静态列，这样将导致所有的主分区都要创建副分区静态列所定义的分区。
--
-- 动态分区可以允许所有的分区列都是动态分区列，但是要首先设置一个参数hive.exec.dynamic.partition.mode ：
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = strict
-- 它的默认值是strick，即不允许分区列全部是动态的，这是为了防止用户有可能原意是只在子分区内进行动态建分区，但是由于疏忽忘记为主分区列指定值了，这将导致一个dml语句在短时间内创建大量的新的分区（对应大量新的文件夹），对系统性能带来影响。
-- 所以我们要设置：

set hive.exec.dynamic.partition.mode = nostrick;
set hive.exec.max.dynamic.partitions.pernode = 100;
set hive.exec.max.dynamic.partitions = 1000;
set hive.exec.max.created.files = 1000;


create table ca_employees
as
select name, salary, address
from learnhive.employees
where state = 'CA';

show tables;
select *
from ca_employees;

insert overwrite local directory '/tmp/ca_employees'
select name, salary, address
from learnhive.employees
where state = 'CA';


from learnhive.employees se
insert overwrite local directory '/tmp/or_employees'
select *
where se.country = 'US'
  and se.state = 'OR'
insert overwrite local directory '/tmp/ca_employees'
select *
where se.country = 'US'
  and se.state = 'CA'
insert overwrite local directory '/tmp/il_employees'
select *
where se.country = 'US'
  and se.state = 'IL';