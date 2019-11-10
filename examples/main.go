package main

import (
	"fmt"
	connect "github.com/kevinsamoei/kafka-connect-go"
)

func main() {
	newConnect := connect.NewConnect("connect:8083")
	exampleConfig := map[string]interface{}{
		"connector.class": "io.confluent.connect.replicator.ReplicatorSourceConnector",
		"topic.whitelist": "users",
		"key.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"value.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"src.kafka.bootstrap.servers": "broker-dc1:29091",
		"src.consumer.group.id": "replicator-dc1-to-dc2-topic1",
		"src.consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
		"src.consumer.confluent.monitoring.interceptor.bootstrap.servers": "broker-dc2:29092",
		"src.kafka.timestamps.producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor",
		"src.kafka.timestamps.producer.confluent.monitoring.interceptor.bootstrap.servers": "broker-dc2:29092",
		"dest.kafka.bootstrap.servers": "broker-dc2:29092",
		"confluent.topic.replication.factor": 1,
		"provenance.header.enable": "true",
		"header.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"tasks.max": "1",
	}
	req := newConnect.CreateConnectorRequest(connect.ConnectorRequest{
		Name:   "example-connector",
		Config: exampleConfig,
	})
	res, err := newConnect.CreateConnector(req)
	if err != nil {
		fmt.Print(err)
	}
	fmt.Print(res)
}
