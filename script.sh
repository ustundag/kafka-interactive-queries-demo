# will be run under kafka bin folder

#./kafka-topics.sh --zookeeper localhost:2181 --alter --topic media-upload-topic \
#--config retention.ms=1000

cd /Users/anil.ustundag/Dev/kafka/kafka-2.7.0-src/bin

./kafka-topics.sh --delete --topic media-upload-topic -zookeeper localhost:2181

./kafka-topics.sh --create --topic media-upload-topic \
-zookeeper localhost:2181 --replication-factor 1 --partitions 3

./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic media-upload-topic --from-beginning \
-property "key.separator= <> " --property "print.key=true" \
--property "print.partition=true" --property "print.timestamp=true" \
--group test_group1
