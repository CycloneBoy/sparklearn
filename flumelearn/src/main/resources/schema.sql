create database mysqlsource;

use mysqlsource;

create table student
(
    id   int(11)      not null auto_increment,
    name varchar(255) not null,
    primary key (`id`)
);

create table flume_meta(
    source_tab varchar(255) not null ,
    currentIndex varchar(255) not null ,
    primary key (`source_tab`)
);

show tables;

insert into student values (1,'zhangsan'),(2,'lisi'),(3,'wangwu'),(4,'zhaoliu');

select * from student;

insert into student( name) values ('xiaoming'),('尼斯');

select * from flume_meta;