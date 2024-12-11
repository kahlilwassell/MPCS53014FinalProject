#!/bin/bash
# Create the "kjwassell_train_crowding" topic
kafka-topics.sh --create --replication-factor 3 --partitions 1 --topic kjwassell_station_entries --bootstrap-server $KAFKABROKERS
# list out the kafka topics to verify kjwassell_station_entries shows up.
kafka-topics.sh --list --bootstrap-server $KAFKABROKERS

