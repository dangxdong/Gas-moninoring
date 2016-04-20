
-- Preparing the gas data, store the data in MySQL and then HDFS, and do some exploration with Hive.

-- Ensure that the hadoop services are started.
-- In MySQL: 

create database Gas;
use Gas;

drop table if exists ethylene_CO;

create table ethylene_CO (ID int not null auto_increment, Timesec float, CO float, Ethylene float, 
                         sr1 float, sr2 float, sr3 float, sr4 float,
                         sr5 float, sr6 float, sr7 float, sr8 float,
                         sr9 float, sr10 float, sr11 float, sr12 float,
                        sr13 float, sr14 float, sr15 float, sr16 float,
						PRIMARY KEY (ID) );
						
show tables;

describe ethylene_CO;

-- Exit MySQL, and run this in terminal:
--  mysqlimport --fields-terminated-by=, --columns='Timesec, CO, Ethylene, sr1, sr2, sr3, sr4, sr5, sr6, sr7, sr8, sr9, sr10, sr11, sr12, sr13, sr14, sr15, sr16' --local -u root -p Gas /home/hduser/Documents/GasData/ethylene_CO.csv 

-- So ethylene_CO is now saved in MySQL

---- In MySQL, see how many rows are there in the data:
select count(*) from ethylene_CO;                         -- 4208262


---- Use Sqoop, transfer the table from mysql to hdfs. In terminal:

--   sqoop-import --connect jdbc:mysql://127.0.0.1/Gas --username root -P --table ethylene_CO


----  Use Hive to get some idea of the dataset:

---- Create the table at its location on HDFS:

-- simply

create external table ethylene_CO (
    ID int, Timesec float, CO float, Ethylene float, 
    sr1 float, sr2 float, sr3 float, sr4 float,
    sr5 float, sr6 float, sr7 float, sr8 float,
    sr9 float, sr10 float, sr11 float, sr12 float,
    sr13 float, sr14 float, sr15 float, sr16 float
)
row format delimited fields terminated by ','
location 'hdfs://localhost:54310/user/hduser/ethylene_CO';

-- then run a few codes:

select count(*) from ethylene_CO;        -- 4208262 rows in total, same as done in MySQL.

select * from ethylene_CO limit 10;      -- verify the data are imported correctly

-- get an idea how many records are there for each distinct combination of CO and Ethylene concentrations

----   create the table first:
drop TABLE  if exists ethylene_CO_count;
CREATE TABLE ethylene_CO_count (
    CO float, Ethylene float, 
    NumRecords int
) row format delimited fields terminated by ',';

-- and then insert the selected new table:  
insert into table ethylene_CO_count
select CO, Ethylene, count(*) as NumRecords from ethylene_CO
group by CO, Ethylene;

-- The above codes returns a table with the count(*) between 10000 and 20000 in each row. This table is stored on hdfs in the Hive folder.

--  See how many distinct combinations of CO and Ethylene concentrations are there:

select count(*) from ethylene_CO_count;                      -- get 73.


-- get the average sensor readings for each unique group, as well as the min and max of Timesec:

drop TABLE  if exists ethylene_CO_avgsr_avg;
CREATE TABLE ethylene_CO_avgsr_avg (
    CO float, Ethylene float, mintime float, maxtime float,
    avgsr1 float, avgsr2 float, avgsr3 float, avgsr4 float,
    avgsr5 float, avgsr6 float, avgsr7 float, avgsr8 float,
    avgsr9 float, avgsr10 float, avgsr11 float, avgsr12 float,
    avgsr13 float, avgsr14 float, avgsr15 float, avgsr16 float
) row format delimited fields terminated by ',';

insert into table ethylene_CO_avgsr_avg
select CO, Ethylene, min(Timesec) as mintime, max(Timesec) as maxtime,
avg(sr1) as avgsr1, avg(sr2) as avgsr2, avg(sr3) as avgsr3, avg(sr4) as avgsr4, 
avg(sr5) as avgsr5, avg(sr6) as avgsr6, avg(sr7) as avgsr7, avg(sr8) as avgsr8, 
avg(sr9) as avgsr9, avg(sr10) as avgsr10, avg(sr11) as avgsr11, avg(sr12) as avgsr12, 
avg(sr13) as avgsr13, avg(sr14) as avgsr14, avg(sr15) as avgsr15, avg(sr16) as avgsr16
from ethylene_CO
group by CO, Ethylene
order by mintime;



------ similarly, get the standard deviations for each of the 73 distinct combinations:

-- In hive:
CREATE TABLE ethylene_CO_stdsr_std (
    CO float, Ethylene float, mintime float, maxtime float,
    stdsr1 float, stdsr2 float, stdsr3 float, stdsr4 float,
    stdsr5 float, stdsr6 float, stdsr7 float, stdsr8 float,
    stdsr9 float, stdsr10 float, stdsr11 float, stdsr12 float,
    stdsr13 float, stdsr14 float, stdsr15 float, stdsr16 float
) row format delimited fields terminated by ',';

insert into table ethylene_CO_stdsr_std
select CO, Ethylene, min(Timesec) as mintime, max(Timesec) as maxtime,
stddev(sr1) as stdsr1, stddev(sr2) as stdsr2, stddev(sr3) as stdsr3, stddev(sr4) as stdsr4, 
stddev(sr5) as stdsr5, stddev(sr6) as stdsr6, stddev(sr7) as stdsr7, stddev(sr8) as stdsr8, 
stddev(sr9) as stdsr9, stddev(sr10) as stdsr10, stddev(sr11) as stdsr11, stddev(sr12) as stdsr12, 
stddev(sr13) as stdsr13, stddev(sr14) as stdsr14, stddev(sr15) as stdsr15, stddev(sr16) as stdsr16
from ethylene_CO
group by CO, Ethylene
order by mintime;

-- turns out the standard deviation is nearly at the same scale of the average values, indicating that the sensor readings are noisy.



