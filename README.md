# MPCS53014FinalProject
This was my Final Project for the UChicago course MPCS53014 Big Data Application Architecture
**Steps for Running the Application**
1) ssh into the cluster `ssh -i /Users/kahlilwassell/.ssh/id_MPCS53014_rsa sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net`
2) start up application `node app.js 3001 https://hbase-mpcs53014-2024.azurehdinsight.net/hbaserest $KAFKABROKERS`
3) start up the kafka consumer to process live updates `spark-submit --class StreamStationEntries --master yarn --deploy-mode cluster --executor-memory 2G --num-executors 3 stream_kafka_station_entries-1.0-SNAPSHOT.jar $KAFKABROKERS`
4) Set up socks proxy to proxy application `https://edstem.org/us/courses/68329/discussion/5855877`
5) run the command for tunneling locally `ssh -i /Users/kahlilwassell/.ssh/id_MPCS53014_rsa -C2qTnNf -D 9876 sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net`
6) navigate to url `http://10.0.0.38:3001`
