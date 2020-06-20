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



-------------------------------------------------------------------
--   第六章 HiveSql 数据查询
-------------------------------------------------------------------

select name, subordinates
from learnhive.employees;

select name, deductions
from learnhive.employees;

select name, address
from learnhive.employees;

select name, subordinates[0]
from learnhive.employees;

select name, deductions["State Taxes"]
from learnhive.employees;

select name, address.city
from learnhive.employees;

select *
from learnhive.stocks
limit 10;

show create table learnhive.stocks;

load data local inpath '/home/sl/learnhive/prog-hive-1st-ed-data/data/stocks/stocks.csv'
into table learnhive.stocks;

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
row format delimited fields terminated by ','
location '/data/stocks';

select *
from learnhive.stocks
limit 10;

select symbol, `price.*`
from learnhive.stocks;

select upper(name),
       salary,
       deductions["Federal Taxes"],
       round(salary * (1 - deductions["Federal Taxes"]))
from learnhive.employees;

select count(*), avg(salary)
from learnhive.employees;

set hive.map.aggr = true;

select count(distinct symbol)
from learnhive.stocks;

select explode(subordinates) as sub
from learnhive.employees;

select upper(name),
       salary,
       deductions["Federal Taxes"],
       round(salary * (1 - deductions["Federal Taxes"]))
from learnhive.employees
limit 2;

select upper(name),
       salary,
       deductions["Federal Taxes"]                       as fed_taxes,
       round(salary * (1 - deductions["Federal Taxes"])) as salary_minus_fed_taxes
from learnhive.employees;

from
(
    select upper(name)                                       as name,
           salary,
           deductions["Federal Taxes"]                       as fed_taxes,
           round(salary * (1 - deductions["Federal Taxes"])) as salary_minus_fed_taxes
    from learnhive.employees
)
e
select e.name, e.salary_minus_fed_taxes
where e.salary_minus_fed_taxes > 70000;

select name,
       salary,
       case
           when salary < 50000.0 then 'low'
           when salary >= 50000.0 and salary < 70000.0 then 'middle'
           when salary >= 70000.0 and salary < 100000.0 then 'high'
           else 'very high'
           end as bracket
from learnhive.employees;

select *
from learnhive.employees;
select *
from learnhive.employees
where country = 'US'
  and state = 'CA';

set hive.exec.mode.local.auto = true;


select upper(name),
       salary,
       deductions["Federal Taxes"]                as fed_taxes,
       salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
from learnhive.employees
where round(salary * (1 - deductions["Federal Taxes"])) > 70000;


select upper(name),
       salary,
       deductions["Federal Taxes"]                as fed_taxes,
       salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
from learnhive.employees
where salary_minus_fed_taxes > 70000;

select e.*
from (
         select upper(name),
                salary,
                deductions["Federal Taxes"]                as fed_taxes,
                salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
         from learnhive.employees) e
where e.salary_minus_fed_taxes > 70000;

select name, salary, deductions["Federal Taxes"]
from learnhive.employees;

select name, salary, deductions["Federal Taxes"]
from learnhive.employees
where deductions["Federal Taxes"] > 0.2;

select name, salary, deductions["Federal Taxes"]
from learnhive.employees
where deductions["Federal Taxes"] > cast(0.2 as float);


select name, address.street
from learnhive.employees
where address.street like '%Ave.';

select name, address.city
from learnhive.employees
where address.city like 'O%';

select name, address.street
from learnhive.employees
where address.street like '%Chi%';

select name, address.street
from learnhive.employees
where address.street rlike '.*(Chicago|Ontario).*';

select year(ymd), avg(price_close)
from learnhive.stocks
where `exchange` = 'NASDAQ'
  and symbol = 'AAPL'
group by year(ymd);

select year(ymd), avg(price_close) as avg_price_close
from learnhive.stocks
where `exchange` = 'NASDAQ'
  and symbol = 'AAPL'
group by year(ymd)
having avg_price_close > 50.0;

select year, avg_price_close
from (select year(ymd)        as year,
             avg(price_close) as avg_price_close
      from learnhive.stocks
      where `exchange` = 'NASDAQ'
        and symbol = 'AAPL'
      group by year(ymd)
     ) s2
where s2.avg_price_close > 50.0;


select a.ymd, a.price_close, b.price_close
from learnhive.stocks as a
         join learnhive.stocks as b
              on a.ymd = b.ymd
where a.symbol = 'AAPL'
  and b.symbol = 'IBM';

select a.ymd, a.price_close, b.price_close
from learnhive.stocks as a
         join learnhive.stocks as b
              on a.ymd <= b.ymd
where a.symbol = 'AAPL'
  and b.symbol = 'IBM';

create
external table if not exists dividends(
    ymd string,
    dividend float
)
partitioned by (`exchange` string,symbol string)
row format delimited fields terminated by ','
location '/data/dividends';

drop table dividends;

create
external table if not exists dividends_tmp(
    `exchange` string,
    symbol string,
    ymd string,
    dividend float
)
row format delimited fields terminated by ','
location '/data/tmp/dividends';

load data local inpath "/home/sl/learnhive/prog-hive-1st-ed-data/data/dividends/dividends.csv"
into table dividends_tmp;

select *
from dividends_tmp
limit 10;

select `exchange`, symbol, count(*)
from dividends_tmp
group by `exchange`, symbol;

select *
from dividends_tmp
where symbol = 'GE';
select *
from dividends_tmp
where symbol = 'IBM';
select *
from dividends_tmp
where symbol = 'AAPL';


from dividends_tmp
insert into table dividends partition (`exchange`='NASDAQ', symbol='AAPL')
select ymd, dividend
where `exchange` = 'NASDAQ'
  and symbol = 'AAPL'
insert into table dividends partition (`exchange`='NYSE', symbol='IBM')
select ymd, dividend
where `exchange` = 'NYSE'
  and symbol = 'IBM'
insert into table dividends partition (`exchange`='NYSE', symbol='GE')
select ymd, dividend
where `exchange` = 'NYSE'
  and symbol = 'GE';


select `exchange`, symbol
from dividends
group by `exchange`, symbol;

select s.ymd, s.symbol, s.price_close, d.dividend
from learnhive.stocks s
         join dividends d on s.ymd = d.ymd and s.symbol = d.symbol
where s.symbol = 'AAPL';

select a.ymd, a.price_close, b.price_close, c.price_close
from learnhive.stocks as a
         join learnhive.stocks as b on a.ymd <= b.ymd
         join learnhive.stocks as c on a.ymd <= c.ymd
where a.symbol = 'AAPL'
  and b.symbol = 'IBM'
  and c.symbol = 'GE';

set hive.strict.checks.cartesian.product = false;
set hive.mapred.mode = nostrict;

set hive.strict.checks.cartesian.product = true;
set hive.mapred.mode = strict;


select s.ymd, s.symbol, s.price_close, d.dividend
from learnhive.stocks s
         join dividends d on s.ymd = d.ymd and s.symbol = d.symbol
where s.symbol = 'AAPL';

select s.ymd, s.symbol, s.price_close, d.dividend
from learnhive.stocks s
         left outer join dividends d on s.ymd = d.ymd and s.symbol = d.symbol
where s.symbol = 'AAPL';

select s.ymd, s.symbol, s.price_close, d.dividend
from learnhive.stocks s
         left outer join dividends d on s.ymd = d.ymd and s.symbol = d.symbol
where s.symbol = 'AAPL'
  and d.`exchange` = 'NASDAQ';

SELECT *
from learnhive.stocks
         join learnhive.dividends;

select /* +MAPJOIN(d) */s.ymd, s.symbol, s.price_close, d.dividend
from learnhive.stocks s
         join learnhive.dividends d on s.ymd = d.ymd and s.symbol = d.symbol
where s.symbol = 'AAPL';

set hive.auto.convert.join;
set hive.auto.convert.join = true;

select s.ymd, s.symbol, s.price_close, d.dividend
from learnhive.stocks s
         join learnhive.dividends d on s.ymd = d.ymd and s.symbol = d.symbol
where s.symbol = 'AAPL';

set hive.optimize.bucketmapJOIN;
set hive.optimize.bucketmapJOIN = true;

select s.ymd, s.symbol, s.price_close
from learnhive.stocks s
order by s.ymd asc, s.symbol desc;

select s.ymd, s.symbol, s.price_close
from learnhive.stocks s
where s.symbol = 'AAPL'
order by s.ymd asc, s.symbol desc;

select s.ymd, s.symbol, s.price_close
from learnhive.stocks s DISTRIBUTE BY S.symbol
SORT by s.ymd asc ,s.symbol
desc;

select s.ymd, s.symbol, s.price_close
from learnhive.stocks s CLUSTER BY S.symbol;


create table numbers
(
    number int
);

describe formatted numbers;

load data local inpath '/home/sl/numbers.txt'
into table numbers;

select *
from numbers;

select *
from learnhive.numbers tablesample ( bucket 3 out of 10
on rand()) s;

select *
from learnhive.numbers tablesample ( bucket 3 out of 10
on number) s;

select *
from learnhive.numbers tablesample ( bucket 1 out of 2
on number) s;

select *
from learnhive.numbers tablesample ( bucket 2 out of 2
on number) s;

create table numbers_bucketed
(
    number int
) clustered by (number) into 3 buckets;

set hive.enforce.bucketing = true;

insert overwrite table learnhive.numbers_bucketed
select *
from learnhive.numbers;

select *
from learnhive.numbers_bucketed tablesample ( bucket 2 out of 3
on number) s;


-------------------------------------------------------------------
--   第7章 HiveSql 视图
-------------------------------------------------------------------
show create table learnhive.employees;

select *
from learnhive.employees;

create view employees_deduction_F(name, deduction) as
select e.name, e.deductions["Federal Taxes"]
from learnhive.employees e;

create view employees_deduction_S(name, deduction) as
select e.name, e.deductions["State Taxes"]
from learnhive.employees e;

create view if
            not exists employees_deduction_S(name,deduction)
            comment "test view"
            tblproperties ('creator'='sl')
as
select e.name, e.deductions["State Taxes"]
from learnhive.employees e;

drop view if exists learnhive.employees_deduction_S;

select explode(deductions) as (dname,deduction)
from learnhive.employees;

select *
from employees_deduction_F;

select *
from employees_deduction_S;

describe formatted learnhive.employees_deduction_S;

alter view learnhive.employees_deduction_S set tblproperties ('create_at'='2020-06-14');

-------------------------------------------------------------------
--   第13章 Hive 函数
--   2020-06-20
-------------------------------------------------------------------

show functions;

describe function concat;

select year(ymd),
       avg(price_close)
from learnhive.stocks
where `exchange` = 'NASDAQ'
  AND symbol = 'AAPL'
group by year(ymd);

show partitions learnhive.stocks;

describe extended learnhive.stocks;
show create table learnhive.stocks;

show tables like 'stock*';

select `array`(1, 2, 3);
select explode(`array`(1, 2, 3));

select name,
       sub
from learnhive.employees lateral view explode(subordinates) subView as sub;

create table if not exists littlebigdata
(
    name string,
    email string,
    bday string,
    ip string,
    gender string,
    anum int
) row format delimited fields terminated by ',';

load data local inpath '/home/sl/learnhive/data/littlebigdata.txt'
into table littlebigdata;

select *
from littlebigdata;

truncate table littlebigdata;

add jar /home/sl/workspace/java/a2019/sparklearn/hivelearn/hive-common/target/hive-common-0.0.1-SNAPSHOT.jar;

create temporary
function zodiac as 'com.cycloneboy.bigdata.hivelearn.common.UDFZodiacSign';
create temporary
function nvl as 'com.cycloneboy.bigdata.hivelearn.common.GenericUDFNvl';

describe function zodiac;
describe function extended zodiac;

select name, bday, zodiac(bday)
from learnhive.littlebigdata;

drop
temporary
function if exists zodiac;


describe function nvl;
describe function extended nvl;

select nvl(1, 2)          as c1,
       nvl(NULL, 5)       as c2,
       nvl(null, 'stuff') as c3;

-- select forx(1,5);

create
temporary
macro sigmoid (x double) 1.0/(1.0+exp(-x));
select sigmoid(2);

-------------------------------------------------------------------
--   第14章 Hive Streaming
--   2020-06-20
-------------------------------------------------------------------
-- create table a(col1 int ,col2 int)
-- 恒等变换,改变类型,投影变换,操作转换,使用分布式内存,由一行产生多行,使用streaming 进行聚合计算,
-- cluster by,distribute by ,sort by
-- GenericMR Tools for streaming to Java