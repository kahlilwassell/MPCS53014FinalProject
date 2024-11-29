#!/bin/bash

# Create the "kjwassell_train_positions" topic
kafka-topics.sh --create --replication-factor 3 --partitions 1 --topic kjwassell_train_positions --bootstrap-server $KAFKABROKERS

# Create the "kjwassell_train_crowding" topic
kafka-topics.sh --create --replication-factor 3 --partitions 1 --topic kjwassell_train_crowding --bootstrap-server $KAFKABROKERS
