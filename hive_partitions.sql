

create table categories_part PARTITIONED BY (category_department_id) as select category_id,category_department_id,category_name from categories ;


create table categories_part(category_id1 int, category_name1 string) partitioned by (category_department_id1 int);

set hive.exec.dynamic.partition.mode=nonstrict;

FROM categories cat
INSERT OVERWRITE TABLE categories_part PARTITION(category_department_id1)
SELECT cat.category_id,cat.category_name,cat.category_department_id
DISTRIBUTE BY category_department_id;

create table categories_part_2(category_id1 int, category_name1 string) partitioned by (actdate String, category_department_id1 int);

FROM categories cat
INSERT OVERWRITE TABLE categories_part_2 PARTITION(actdate="2018-04-11",category_department_id1)
SELECT cat.category_id,cat.category_name,cat.category_department_id
DISTRIBUTE BY category_department_id;

show partitions categories_part_2;




