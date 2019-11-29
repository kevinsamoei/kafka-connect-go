package connect

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	connectURL = "connect:8083"
	testConnectorConfig = map[string]interface{}{
		"connector.class": "io.confluent.connect.replicator.ReplicatorSourceConnector",
		"topic.whitelist": "users",
		"key.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"value.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"src.kafka.bootstrap.servers": "source-kafka:9091",
		"src.consumer.group.id": "replicator-dc1-to-dc2-topic1",
		"src.consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
		"src.consumer.confluent.monitoring.interceptor.bootstrap.servers": "destination-kafka:9092",
		"src.kafka.timestamps.producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor",
		"src.kafka.timestamps.producer.confluent.monitoring.interceptor.bootstrap.servers": "destination-kafka:9092",
		"dest.kafka.bootstrap.servers": "destination-kafka:9092",
		"confluent.topic.replication.factor": 1,
		"provenance.header.enable": "true",
		"header.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"tasks.max": "1",
	}
)

func sleep()  {
	time.Sleep(20 * time.Second)
}

func TestCreateConnectorRequest(t *testing.T) {
	// create connect
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-create-connector-request",
		Config: testConnectorConfig,
	})

	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Name, req.Name)
	assert.Equal(t, response.Config, req.Config)
	assert.Equal(t, response.Code, 201)
}

func TestGetConnectors(t *testing.T) {
	// create a connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-get-connectors",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// get connector
	getConnectorResp, err := connect.GetConnectors()
	assert.NoError(t, err)
	assert.Equal(t, getConnectorResp.Code, 200)
	assert.Contains(t, getConnectorResp.Connectors, req.Name)

}

func TestCreateConnector(t *testing.T) {
	connect := NewConnect(connectURL)

	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-create-connector",
		Config: testConnectorConfig,
	})

	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)
}

func TestGetConnector(t *testing.T) {
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-get-connector",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// get the created connector
	getConnectorResponse, err := connect.GetConnector(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, getConnectorResponse.Code, 200)
	assert.Equal(t, getConnectorResponse.Name, response.Name)
}

func TestGetConnectorConfig(t *testing.T) {

	// create a connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-get-connector-config",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// get it's config
	getConfigResp, err := connect.GetConnectorConfig(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, getConfigResp.Code, 200)
	assert.Equal(t, getConfigResp.Config, req.Config)
}

func TestConnect_UpdateConnectorConfig(t *testing.T) {
	// create connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-update-connector-config",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// create update config request
	updateConfig := map[string]interface{}{
		"connector.class": "io.confluent.connect.replicator.ReplicatorSourceConnector",
		"topic.whitelist": "users",
		"key.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"value.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"src.kafka.bootstrap.servers": "source-kafka:9091",
		"src.consumer.group.id": "replicator-dc1-to-dc2-topic1",
		"src.consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
		"src.consumer.confluent.monitoring.interceptor.bootstrap.servers": "destination-kafka:9092",
		"src.kafka.timestamps.producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor",
		"src.kafka.timestamps.producer.confluent.monitoring.interceptor.bootstrap.servers": "destination-kafka:9092",
		"dest.kafka.bootstrap.servers": "destination-kafka:9092",
		"confluent.topic.replication.factor": 1,
		"provenance.header.enable": "true",
		"header.converter": "io.confluent.connect.replicator.util.ByteArrayConverter",
		"tasks.max": "10",
	}

	updateReq := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   req.Name,
		Config: updateConfig,
	})

	// update the connector config
	updateConnectorResp, err := connect.UpdateConnectorConfig(updateReq)
	assert.NoError(t, err)
	assert.Equal(t, updateConnectorResp.Code, 200)
	assert.Equal(t, updateConnectorResp.Config, updateConfig)
	assert.Equal(t, updateConnectorResp.Config["tasks.max"], "10")
}

func TestConnect_GetConnectorStatus(t *testing.T) {
	// create a connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-get-connector-status",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// get it's status
	statusResp, err := connect.GetConnectorStatus(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, statusResp.Code, 200)
	assert.Equal(t, statusResp.ConnectorStatus["state"], "RUNNING")
}

func TestConnect_RestartConnector(t *testing.T) {
	// create connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-restart-connector",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// restart it
	restartResp, err := connect.RestartConnector(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, restartResp.Code, 200)
}

func TestConnect_PauseConnector(t *testing.T) {
	// create connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-pause-connector",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// pause the connector
	pauseRes, err := connect.PauseConnector(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, pauseRes.Code, 202)

	// sleep to allow for rebalance
	sleep()

	// get its status. Should be PAUSED
	getStatusRes, err := connect.GetConnectorStatus(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, getStatusRes.Code, 200)
	assert.Equal(t, getStatusRes.ConnectorStatus["state"], "PAUSED")
}

func TestConnect_ResumeConnector(t *testing.T) {
	// create connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-restart-connector",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// pause the connector
	pauseRes, err := connect.PauseConnector(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, pauseRes.Code, 202)

	// sleep to allow for rebalance
	sleep()

	// get its status. Should be PAUSED
	getStatusRes, err := connect.GetConnectorStatus(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, getStatusRes.Code, 200)
	assert.Equal(t, getStatusRes.ConnectorStatus["state"], "PAUSED")

	// sleep to allow for rebalance
	sleep()

	// resume connector
	resumeResp, err := connect.ResumeConnector(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, resumeResp.Code, 202)

	// sleep to allow for rebalance
	sleep()

	// get the status now. Should be in RUNNING state
	getStatusResumeRes, err := connect.GetConnectorStatus(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, getStatusResumeRes.Code, 200)
	assert.Equal(t, getStatusResumeRes.ConnectorStatus["state"], "RUNNING")
}

func  TestConnect_DeleteConnector(t *testing.T) {
	// create connector
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-delete-connector",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	// delete connector
	deleteRes, err := connect.DeleteConnector(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, deleteRes.Code, 204)

	// sleep to allow for rebalance
	sleep()

	// get connector. should not exist
	getConnectorRes, err := connect.GetConnector(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, getConnectorRes.Code, 404)
}

func TestConnect_GetConnectorTasks(t *testing.T) {
	connect := NewConnect(connectURL)
	req := connect.CreateConnectorRequest(ConnectorRequest{
		Name:   "test-get-connector-tasks",
		Config: testConnectorConfig,
	})
	response, err := connect.CreateConnector(req)
	assert.NoError(t, err)
	assert.Equal(t, response.Code, 201)

	// sleep to allow for rebalance
	sleep()

	getTaskResp, err := connect.GetConnectorTasks(req.Name)
	assert.NoError(t, err)
	assert.Equal(t, getTaskResp.Code, 200)
}

func TestConnect_GetConnectorTaskStatus(t *testing.T){
	t.Skip("Test me")
}

func TestConnect_RestartConnectorTask(t *testing.T) {
	t.Skip("Test me")
}
func TestConnect_GetConnectorPlugins(t *testing.T) {
	t.Skip("Test me")
}
func TestConnect_ValidatePluginConfig(t *testing.T) {
	t.Skip("Test me")
}
