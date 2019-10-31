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


-- 创建一张分区表
create table student6
(
    id   int,
    name string
)
    partitioned by (month string)
    row format delimited fields terminated by '\t';

-- 基本插入数据
insert into table student6 partition (month = '201709')
values (1, 'wangwu'),
       (2, 'zhaoliu');

-- 基本插入(根据单张表查询结果)
insert overwrite table student6 partition (month = '201708')
select id, name
from student6
where month = '201709';

-- 基本查询
select *
from student6;

-- 多表(多分区)插入模式(根据多张表查询结果)
from student6
insert
overwrite
table
student6
partition
(
month = '201707'
)
select id, name
where month = '201709'
insert
overwrite
table
student6
partition
(
month = '201706'
)
select id, name
where month = '201709';

-- 根据查询结果创建表
create table if not exists student7 as
select id, name
from student6;

-- 创建表,并指定在hdfs上的位置
create external table if not exists student8
(
    id   int,
    name string
) row format delimited fields terminated by '\t' location '/student';

-- import 数据到指定hive表中
import table student7 partition (month='201704') from '/user/hive/warehouse/export/student';

-- 将查询的结果导出到本地
insert overwrite local directory '/opt/module/datas/export/student'
select *
from student;

-- 将查询的结果格式化导出到本地
insert overwrite local directory '/opt/module/datas/export/student1'
    row format delimited fields terminated by '\t'
select *
from student;

-- 将查询的结果导出到hdfs
insert overwrite directory '/user/cycloneboy/student2'
    row format delimited fields terminated by '\t'
select *
from student;

-- hadoop 命令导出到本地
-- dfs -get /user/cycloneboy/student2/000000_0 /opt/module/datas/export/student3.txt;

-- hive shell 导出数据
hive -e 'select * from student;' > /opt/module/datas/export
/student4.txt

-- export导出到hdfs上
export table default.student to '/user/hive/warehouse/export/student5';

-- 清楚表中的数据
-- Truncate只能删除管理表，不能删除外部表中数据
truncate table student7;

-- 表格查询

-- 创建部门表
create table if not exists dept
(
    deptno int,
    danme  string,
    loc    int
)
    row format delimited fields terminated by '\t';

-- 创建员工表
create table if not exists emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    row format delimited fields terminated by '\t';

-- 导入数据
load data local inpath '/opt/module/datas/dept.txt' into table dept;
load data local inpath '/opt/module/datas/emp.txt' into table emp;

-- 全表查询
select *
from emp;
select empno, ename
from emp;

-- 查询别名
select ename as name, deptno dn
from emp;

-- 常用函数
select count(*) cnt
from emp;
select max(sal) max_sal
from emp;
select sum(sal) sum_sal
from emp;
select avg(sal) avg_sal
from emp;

-- limit 子句用于限制返回的行数
select *
from emp
limit 5;

-- where 语句
-- 查询出薪水大于1000的所有员工
select *
from emp
where sal > 1000;

--（1）查询出薪水等于5000的所有员工
select *
from emp
where sal = 5000;

--（2）查询工资在500到1000的员工信息
select *
from emp
where sal between 500 and 1000;

--（3）查询comm为空的所有员工信息
select *
from emp
where comm is null;

--（4）查询工资是1500或5000的员工信息
select *
from emp
where sal in (1500, 5000);

-- Like和RLike
--（1）查找以2开头薪水的员工信息
select *
from emp
where sal like '2%';

-- (2）查找第二个数值为2的薪水的员工信息
select *
from emp
where sal like '_2%';

--（3）查找姓名中含有A的员工信息
select *
from emp
where ename rlike '[A]';

-- 逻辑运算符（And/Or/Not）
--（1）查询薪水大于1000，部门是30
select *
from emp
where sal > 1000
  and deptno = 30;

--（2）查询薪水大于1000，或者部门是30
select *
from emp
where sal > 1000
   or deptno = 30;

--（3）查询除了20部门和30部门以外的员工信息
select *
from emp
where deptno not in (20, 30);

-- Group By语句
--（1）计算emp表每个部门的平均工资
select t.deptno, avg(t.sal) avg_sal
from emp t
group by t.deptno;

--（2）计算emp每个部门中每个岗位的最高薪水
select t.deptno, t.job, max(t.sal) max_sal
from emp t
group by t.deptno, t.job;

-- having语句

--（1）求每个部门的平均工资
select deptno, avg(sal) avg_sal
from emp
group by deptno;

--（2）求每个部门的平均薪水大于2000的部门
select deptno, avg(sal) avg_sal
from emp
group by deptno
having avg_sal > 2000;

-- join 语句
-- 等值join
-- 根据员工表和部门表中的部门编号相等，查询员工编号、员工名称和部门名称；
select e.empno, e.ename, d.deptno, d.dname
from emp e
         join dept d on e.deptno = d.deptno;

-- 左外连接
select e.empno, e.ename, d.deptno
from emp e
         left join dept d on e.deptno = d.deptno;

-- 右外连接
select e.empno, e.ename, d.deptno
from emp e
         right join dept d on e.deptno = d.deptno;

-- 满外连接
select e.empno, e.ename, d.deptno
from emp e
         full join dept d on e.deptno = d.deptno;

-- 多表连接
-- (1) 创建位置表
create table if not exists location
(
    loc      int,
    loc_name string
)
    row format delimited fields terminated by '\t';

-- (2) 导入数据
load data local inpath '/opt/module/datas/location.txt' into table location;

-- (3) 基本查询
select *
from location;

-- (4) 多表联合查询
select e.ename, d.dname, l.loc_name
from emp e
         join dept d on d.deptno = e.deptno
         join location l on d.loc = l.loc;

-- 笛卡尔积
select empno, dname
from emp,
     dept;

-- 排序
--（1）查询员工信息按工资升序排列
select *
from emp
order by sal;

--（2）查询员工信息按工资降序排列
select *
from emp
order by sal desc;

-- 多个列排序
-- 按照部门和工资牲升序排列
select ename, deptno, sal
from emp
order by deptno, sal;

-- 每个MapReduce内部排序（Sort By）
-- 1．设置reduce个数
set mapreduce.job.reduces=3;

-- 2．查看设置reduce个数
set mapreduce.job.reduces;

-- 3．根据部门编号降序查看员工信息
select *
from emp sort by deptno desc;

-- 4．将查询结果导入到文件中（按照部门编号降序排序）
insert overwrite local directory '/opt/module/datas/export/sortby-result'
select *
from emp sort by deptno desc;

-- 分区排序(distribute by )

-- 先按照部门编号分区，再按照员工编号降序排序
insert overwrite local directory '/opt/module/datas/distribute-result'
select *
from emp distribute by deptno sort by empno desc;

-- cluster by
-- 当distribute by和sorts by字段相同时，可以使用cluster by方式。
-- cluster by除了具有distribute by的功能外还兼具sort by的功能。但是排序只能是升序排序，不能指定排序规则为ASC或者DESC。

select *
from emp cluster by deptno;
select *
from emp distribute by deptno sort by deptno;

-- 分桶及抽样查询
-- 分桶表数据存储

-- 1 创建分桶表
create table stu_buck
(
    id   int,
    name string
)
    clustered by (id)
        into 4 buckets
    row format delimited fields terminated by '\t';

-- 2 查询表结构
desc formatted stu_buck;

-- 3 导入数据到分桶表中
load data local inpath '/opt/module/datas/student.txt' into table stu_buck;

-- 查询
select *
from stu_buck;

--  创建表
create table stu
(
    id   int,
    name string
)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/datas/student.txt' into table stu;

-- 清空表
truncate table stu_buck;
select *
from stu_buck;

insert into table stu_buck
select id, name
from stu;

-- 设置属性
set hive.enforce.bucketing=true;
set mapreduce.job.reduces=-1;

-- 分桶抽样查询
select *
from stu_buck tablesample (bucket 1 out of 4 on id);

-- 其他常用查询函数
-- 3.查询：如果员工的comm为NULL，则用-1代替
select comm, nvl(comm, -1)
from emp;

-- 4.查询：如果员工的comm为NULL，则用领导id代替
select comm, nvl(comm, mgr)
from emp;


-- 创建表格
create table emp_sex
(
    name    string,
    dept_id string,
    sex     string
)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/datas/emp_sex.txt' into table emp_sex;

-- 求出不同部门男女各多少人
select dept_id,
       sum(case sex when '男' then 1 else 0 end) male_count,
       sum(case sex when '女' then 1 else 0 end) female_count
from emp_sex
group by dept_id;

-- 行转列
-- 创建表
create table person_info
(
    name          string,
    constellation string,
    blood_type    string
)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/datas/constellation.txt' into table person_info;

-- 把星座和血型一样的人归类到一起
select name,
       concat(constellation, ",", blood_type) base
from person_info;


select t1.base,
       concat_ws("|", collect_set(t1.name)) name
from (select name, concat(constellation, ",", blood_type) base from person_info) t1
group by t1.base;

-- 创建电影表并导入数据
create table moive_info
(
    moive    string,
    category array<string>
)
    row format delimited fields terminated by '\t'
        collection items terminated by ",";

load data local inpath '/opt/module/datas/moive.txt' into table moive_info;

select *
from moive_info;

-- 将电影分类中的数组数据展开
select moive,
       category_name
from moive_info
         lateral view explode(category) table_tmp as category_name;

-- 创建消费表格并导入数据
create table business1
(
    name      string,
    orderdate string,
    cost      int
)
    row format delimited fields terminated by ',';

load data local inpath '/opt/module/datas/business.txt' into table business1;

select *
from business1;

--（1）查询在2017年4月份购买过的顾客及总人数
select name, count(*) over ()
from business1
where substring(orderdate, 1, 7) = '2017-04'
group by name;

-- （2）查询顾客的购买明细及月购买总额
select name, orderdate, cost, sum(cost) over (partition by month(orderdate))
from business1;

-- (3)上述的场景, 将每个顾客的cost按照日期进行累加
select name,
       orderdate,
       cost,
       sum(cost) over ()                                                                                 as sample1,--所有行相加
       sum(cost) over (partition by name)                                                                as sample2,--按name分组，组内数据相加
       sum(cost) over (partition by name order by orderdate)                                             as sample3,--按name分组，组内数据累加
       sum(cost)
           over (partition by name order by orderdate rows between UNBOUNDED PRECEDING and current row ) as sample4,--和sample3一样,由起点到当前行的聚合
       sum(cost)
           over (partition by name order by orderdate rows between 1 PRECEDING and current row)          as sample5, --当前行和前面一行做聚合
       sum(cost)
           over (partition by name order by orderdate rows between 1 PRECEDING AND 1 FOLLOWING )         as sample6,--当前行和前边一行及后面一行
       sum(cost)
           over (partition by name order by orderdate rows between current row and UNBOUNDED FOLLOWING ) as sample7 --当前行及后面所有行
from business1;

-- (4) 查看顾客上次的购买时间
select name,
       orderdate,
       cost,
       lag(orderdate, 1, '1900-01-01')
           over (partition by name order by orderdate) as time1,
       lag(orderdate, 2)
           over (partition by name order by orderdate) as time2
from business1;

-- (5) 查询前20%的时间的订单信息
select name, orderdate, cost, ntile(5) over (order by orderdate) sorted
from business1;

select *
from (select name, orderdate, cost, ntile(5) over (order by orderdate) sorted from business1) t
where sorted = 1;


-- rank()
-- 创建数据表并导入数据
create table score
(
    name    string,
    subject string,
    score   int
)
    row format delimited fields terminated by '\t';

load data local inpath '/opt/module/datas/score.txt' into table score;

-- 计算每门成绩的排名
select name,
       subject,
       rank() over (partition by subject order by score desc)       rp,
       dense_rank() over (partition by subject order by score desc) drp,
       row_number() over (partition by subject order by score desc) rmp
from score;

-- 查询系统内置函数
show functions;
desc function upper;

desc function extended upper;
