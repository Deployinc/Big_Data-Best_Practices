CREATE TABLE access_logs (
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\"))?",
  "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s"
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/data/apache_logs/small/*' OVERWRITE INTO TABLE access_logs;

select distinct host from access_logs;
select host, count(*) as ip_count from access_logs group by host order by ip_count desc;
select host, count(*) as ip_count from access_logs where lower(agent) like '%mozilla%' group by host order by ip_count desc;

