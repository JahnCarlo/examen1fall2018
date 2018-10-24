CREATE TABLE student (sch_region STRING, sch_district STRING, sch_ID INT, sch_name STRING, sch_level STRING, sex STRING, std_ID INT) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

CREATE TABLE school (sch_region STRING, sch_district STRING, sch_city STRING, sch_id INT, sch_name STRING, sch_level STRING, sch_serial INT) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH './escuelasPR.csv' OVERWRITE INTO TABLE school;
LOAD DATA LOCAL INPATH './studentsPR.csv' OVERWRITE INTO TABLE student;

#Question 1
insert overwrite local directory './results'
row format delimited
fields terminated by ","
select school.sch_region, school.sch_city, count(*)
from student join school on (student.sch_id = school.sch_id)
group by school.sch_region, school.sch_city;

#Question2
insert overwrite local directory './results'
row format delimited
fields terminated by ","
select sch_city, sch_level, count(*)
from school
group by sch_city, sch_level;

#Question3
insert overwrite local directory './results'
row format delimited
fields terminated by ","
select count(*)
from student join school on (student.sch_id = school.sch_id)
where student.sex = 'F' and school.sch_city = 'Ponce' and school.sch_level ='Superior';

#Question4
insert overwrite local directory './results'
row format delimited
fields terminated by ","
select sch_region, sch_district, sch_city, count(*)
from student join school on (student.sch_id = school.sch_id)
where student.sex = 'M'
group by school.sch_region, school.sch_district, school.sch_city;
