--aws s3 cp apps_info s3://bi.analytics/shlomi/tables/historical_apps_info/apps_info

CREATE EXTERNAL TABLE `historical_apps_info`( 
bundle string,
category string,
original_categories string,
price float
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
LOCATION 
's3n://spotad.audience/prd/app_categories/historical_apps_info/'
TBLPROPERTIES ( 
'transient_lastDdlTime'='1475147184');

select * from historical_apps_info limit 5;
select count(*) from historical_apps_info;


CREATE EXTERNAL TABLE `historical_apps_users`( 
 auctionid string,
 exchange2 string,
 bundle string,
 idfa string,
 os string,
 osv string,
 make string,
 model string,
 category string,
 language string,
 country string,
 state string,
 city string,
 day_ts bigint,
 external_category string,
 original_external_categories string,
 price float
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
LOCATION 
's3n://bi.analytics/shlomi/tables/historical_apps_users/'
TBLPROPERTIES ( 
'transient_lastDdlTime'='1475147184');

insert into table historical_apps_users
select total.*,
info.category,
info.original_categories,
info.price
from history_total total
join historical_apps_info info
on total.bundle = info.bundle;

select * from historical_apps_users limit 10;
select exchange2, count(*), count(distinct idfa), count(distinct bundle) from historical_apps_users group by exchange2;
/*
omax	119487321	31690622	5998
mopub	286788174	81480914	7190
adx	420386851	159941271	37729
rubicon	849655386	111447004	4016
pubmatic	225679484	68369358	1513
openx	12174514	6845121	464
*/


