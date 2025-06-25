#!/bin/sh

. $HOME/.profile

cd
mkdir logs 2> /dev/null
cd  bin/kafka_2.13-2.6.0/bin

./kafka-console-consumer.sh --bootstrap-server vdb1:9092 --topic console_messages --from-beginning | tee -a $HOME/logs/kafka.log

