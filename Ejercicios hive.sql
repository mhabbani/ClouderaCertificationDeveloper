Ejercicios hive

-------  Creacion tablas

create table prueba1(col1 string, col2 bigint, col3 date);

alter table prueba1 change col2 colimn2 int;

--- distintos delimitadores

create table prueba2(col1 string, col2 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
insert into prueba2 values ("XXXX", "YYYYYY");
select * from pruebas2

--- distintos formatos almacenamiento

create table prueba3(col1 int, col2 bigint, col3 String) stored as avro;
insert into prueba3 values (1, 12121, "prueba");
select * from pruebas3

create table prueba4(col1 int, col2 bigint, col3 String) stored as parquet tblproperties("parquet.compression"="snappy");


----  Externa

sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --table orders --target-dir hdfs:/user/cloudera/PruebasHive/orders1 --where "order_id < 100" --as-avrodatafile --compress --compression-codec snappy

create external table orders1(order_id int, order_date bigint, order_customer_id int, order_status string) stored as avro location "hdfs:/user/cloudera/PruebasHive/orders1"


sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --table orders --target-dir hdfs:/user/cloudera/PruebasHive/orders2 --where "order_id < 100" --fields-terminated-by '\t'

create external table orders2(order_id int, order_date string, order_customer_id int, order_status string) row format delimited fields terminated by "\t" location "hdfs:/user/cloudera/PruebasHive/orders2"

----  Particionada

create table prueba4(col1 int, col2 string) partitioned by (col3 string);

--- tipos complejos

create table prueba5(col1 int, col2 struct <col3: int, col4: string>);

INSERT INTO TABLE prueba5 SELECT 1, NAMED_STRUCT('col3',123,'col4','GoldStreet') AS col2 ;


