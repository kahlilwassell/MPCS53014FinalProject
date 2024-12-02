#!/bin/bash
# Create the "kjwassell_train_crowding" topic
kafka-topics.sh --create --replication-factor 3 --partitions 1 --topic kjwassell_train_arrivals --bootstrap-server $KAFKABROKERS
# list out the kafka topics
kafka-topics.sh --list --bootstrap-server $KAFKABROKERS

