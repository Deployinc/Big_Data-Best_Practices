##Building application:

$ mvn clean install

###Running examples:

#####Creating folder on hdfs

$ hdfs dfs -mkdir -p data/apache_logs/small

#####Copying data to hdfs

$ hdfs dfs -cp dataset/employer_access_log.2014.04.1[567] data/apache_logs/small/

#####Starting tasks

$ hadoop jar target/MapReduce-0.0.1-SNAPSHOT.jar com.deploy.hadoop.MapReduce.UserImpressions.UserImpressions data/apache_logs/small/* count_results/test_run

$ hadoop jar target/MapReduce-0.0.1-SNAPSHOT.jar com.deploy.hadoop.MapReduce.UserImpressionsFiltered.UserImpressions data/apache_logs/small/* count_results/test_run1 AGENT mozilla


#####Runing HBase example

$ export HADOOP_CLASSPATH=`/usr/bin/hbase classpath`

$ hadoop jar target/MapReduce-0.0.1-SNAPSHOT.jar com.deploy.hadoop.HBase.HBaseClient
