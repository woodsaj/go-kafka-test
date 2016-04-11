#!/bin/bash

IP=$(ip addr show dev docker0 scope global |grep inet|awk '{print $2}'|cut -d\/ -f1)

docker run -d --name kafka -p 2181:2181 -p 9092:9202 --env ADVERTISED_HOST=$IP woodsaj/kafka

sleep 10;

docker exec -t -i kafka /opt/kafka_2.11-0.9.0.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic test
