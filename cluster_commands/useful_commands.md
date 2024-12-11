# Useful Commands for the MPCS53014 cluster
**SSH Into Hive/HBase/Spark/Kafka server from local**
`ssh -i /Users/kahlilwassell/.ssh/id_MPCS53014_rsa sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net`
**Open Hive with Beeline to query**
`beeline -u 'jdbc:hive2://10.0.0.50:10001/;transportMode=http'`
**Open Hbase Shell**
`hbase shell`
**Create a KafkaTopic**
`kafka-topics.sh --create --replication-factor 3 --partitions 1 --topic <topic_name> --bootstrap-server $KAFKABROKERS`
**Delete a kafka topic**
`kafka-topics.sh --delete --topic kjwassell_station_entries --bootstrap-server $KAFKABROKERS`
**See A List of Kafka Topics on the Cluster**
`kafka-topics.sh --bootstrap-server $KAFKABROKERS --list`
**Set Up a dummy Consumer for the Kafka Topic**
`kafka-console-consumer.sh --bootstrap-server $KAFKABROKERS --topic <topic_name> --from-beginning`
**Command to tunnel to web server** 
`ssh -i /Users/kahlilwassell/.ssh/id_MPCS53014_rsa -C2qTnNf -D 9876 sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net`
**Command to run my web application**
`node app.js 3001 https://hbase-mpcs53014-2024.azurehdinsight.net/hbaserest $KAFKABROKERS`
**Command to run Kafka Data Ingestor**
`node cta_station_entry_consumer.js $KAFKABROKERS`
**Steps to set up the application**
`https://edstem.org/us/courses/68329/discussion/5855877`

`select * from <table> limit 10;`
select * from kjwassell_cta_ridership_with_day_hbase limit 10;
select * from kjwassell_cta_station_view_hbase limit 10;
select * from kjwassell_cta_total_rides_by_day_hbase limit 10;
kafka-console-consumer.sh --bootstrap-server $KAFKABROKERS --topic kjwassell_station_entries --from-beginning