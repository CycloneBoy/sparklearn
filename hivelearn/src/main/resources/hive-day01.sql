-- create user 'hive'@'%' identified 'hive';

-- 创建一个账号：用户名为hive，密码为hive
-- create user 'hive'@'%' identified by 'hive';

-- hive 连接 beeline
-- !connect jdbc:hive2://localhost:10000

-- 将权限授予host为%即所有主机的hive用户
-- grant all privileges on *.* to 'hive'@'%' identified by 'hive' with  grant  option;
-- GRANT ALL PRIVILEGES ON *.* to 'hive'@'%' IDENTIFIED BY 'hive' WITH GRANT OPTION;


-- 10.30
create table student
(
    id   int,
    name string
)
    row format delimited fields terminated by '\t';

-- test
create table test
(
    name     string,
    friends  array<string>,
    children map<string, int>,
    address  struct<street:string, city:string>
)
    row format delimited fields terminated by ','
        collection items terminated by '_'
        map keys terminated by ':'
        lines terminated by '\n';

-- 普通创建表
create table if not exists student2
(
    id   int,
    name string
)
    row format delimited fields terminated by '\t'
    stored as textfile
    location '/user/hive/warehouse/student2';

-- 根据查询结果创建表
create table if not exists student3 as
select id, name
from student;


-- 根据已经存在的表结构创建表
create table if not exists student4 like student;

-- 创建外部表
create external table stu_external
(
    id   int,
    name string
)
    row format delimited fields terminated by '\t'
    location '/student';

-- 创建分区表
create table dept_partition
(
    deptno int,
    dname  string,
    loc    string
)
    partitioned by (month string)
    row format delimited fields terminated by '\t';

-- 加载数据到分区表
load data local inpath '/opt/module/datas/dept.txt' into table default.dept_partition partition (month = '201709');
load data local inpath '/opt/module/datas/dept.txt' into table default.dept_partition partition (month = '201708');
load data local inpath '/opt/module/datas/dept.txt' into table default.dept_partition partition (month = '201707');

-- 多分区联合查询
select *
from dept_partition
where month = '201709'
union
select *
from dept_partition
where month = '201708'
union
select *
from dept_partition
where month = '201707';

-- 创建单个分区
alter table dept_partition
    add partition (month = '201706');

-- 同时创建多个分区
alter table dept_partition
    add partition (month = '201705') partition (month = '201704');

-- 删除单个分区
alter table dept_partition
    drop partition (month = '201704');

-- 同时删除多个分区
alter table dept_partition
    drop partition (month = '201705') , partition (month = '201706');

-- 查看分区表有多个分区
show partitions dept_partition;

-- 查看分区表结构
desc formatted dept_partition;

-- 创建二级分区表
create table dept_partition2
(
    deptno int,
    dname  string,
    loc    string
)
    partitioned by (month string,day string)
    row format delimited fields terminated by '\t';

-- 加载数据到二级分区表
load data local inpath '/opt/module/datas/dept.txt' into table
    default.dept_partition2 partition (month = '201709',day = '13');


-- 查询分区数据
select *
from dept_partition2
where month = '201709'
  and day = '13';

-- 执行修复命令
msck repair table dept_partition2;

-- hdfs上传数据
dfs -put /opt/module/datas/dept.txt /user/hive/warehouse/dept_partition2/month=201709/day=12;

-- 添加分区
alter table dept_partition2
    add partition (month = '201709',day = '11');

-- 查询分区数据
select *
from dept_partition2
where month = '201709'
  and day = '11';


-- 上传数据
load data local inpath '/opt/module/datas/dept.txt' into table dept_partition2 partition (month = '201709',day = '10');

-- 重命名表名称
alter table dept_partition2
    rename to dept_partition3;

-- 查询表结构
desc dept_partition;

-- 添加列
alter table dept_partition
    add columns (deptdesc string);

-- 更新列
alter table dept_partition
    change column deptdesc descint int;

-- 替换列
alter table dept_partition
    replace columns (deptno string, dname string, loc string);

-- 删除表
drop table dept_partition;

-- 创建一张表
create table student
(
    id   string,
    name string
) row format delimited fields terminated by '\t';

-- 加载本地文件到hive
load data local inpath '/opt/module/datas/student.txt' into table default.student;

-- 上传文件到HDFS
dfs -put /opt/module/datas/student.txt /user/cycloneboy/hive;

-- 加载HDFS上数据
load data inpath '/user/cycloneboy/hive/student.txt' into table default.student;

-- 查询
select *
from student5;

-- 加载HDFS上数据覆盖表中已有的数据
load data inpath '/user/cycloneboy/hive/student.txt' overwrite into table default.student;
