package connect

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	logger "github.com/sirupsen/logrus"
	"os"
	"strconv"

	"net"
	"net/http"
	"time"
)

// connect holds the default configs for the connect client
type connect struct {
	client *resty.Client
	connectHost string
}

// NewConnect creates a new instance of connect
func NewConnect(url string) Connect {
	connect := new(connect)
	host := fmt.Sprintf("http://%s", url)
	transport := createTransport()
	httpClient := createHttpClient(transport)
	restClient := resty.NewWithClient(httpClient)

	restClient.SetError(ErrorResponse{}).
		SetHostURL(host).
		SetHeader("Content-Type", "application/json").
		SetRetryCount(5).
		AddRetryCondition(func(r *resty.Response, err error) bool {
			return r.StatusCode() == 404
		})
	connect.client = restClient
	connect.connectHost = host

	return connect
}
// set up the logger
func init() {
	// Log as JSON instead of the default ASCII formatter.
	logger.SetFormatter(&logger.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	logger.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	logger.SetLevel(logger.InfoLevel)
}

// GetConnectors gets a list of all active connectors
// curl -i -H "Accept:application/json" http://localhost:8083/connectors/
// https://docs.confluent.io/current/connect/references/restapi.html#get--connectors
func (c *connect) GetConnectors() (*GetAllConnectorsResponse, error) {
	// get connectors
	response := new(GetAllConnectorsResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		Get("connectors/")

	if err != nil {
		logger.Errorf("Could not get connectors %v", err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Get connectors failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("get connectors error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// CreateConnector creates a kafka connector
// curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @replicator.json
// https://docs.confluent.io/current/connect/references/restapi.html#post--connectors
func (c *connect) CreateConnector(req ConnectorRequest) (*ConnectorResponse,error) {
	body, err := json.Marshal(req)
	if err != nil {
		logger.WithError(err).Errorf("error marshalling the connector request: %v", err)
		return nil, err
	}

	response := new(ConnectorResponse)
	resp, err := c.client.NewRequest().
		SetBody(body).
		SetResult(&response).
		Post("connectors")
	if err != nil {
		logger.WithError(err).Error("could not create client")
		return nil, err
	}

	logger.WithField("create connector response", resp.String()).Info("Create connector response")

	if resp.StatusCode() >= 400 {
		logger.Errorf("Create connector failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("create connector error: %v", resp.String())
	}
	response.Code = resp.StatusCode()

	return response, nil
}

// GetConnector is used to get information about a connector.
// curl -i -H "Accept:application/json" http://localhost:8083/connectors/(string:name)
// https://docs.confluent.io/current/connect/references/restapi.html#get--connectors-(string-name)
func (c *connect) GetConnector(connectorName string) (*ConnectorResponse,error) {
	// get connector
	response := new(ConnectorResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Get("connectors/{name}/")

	if err != nil {
		logger.Errorf("Could not get connector %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Get connector failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("get connector error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// GetConnectorConfig gets the configuration for the connector
// https://docs.confluent.io/current/connect/references/restapi.html#get--connectors-(string-name)-config
func (c *connect) GetConnectorConfig(connectorName string) (*GetConnectorConfigResponse, error) {
	response := new(GetConnectorConfigResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Get("connectors/{name}/config")

	if err != nil {
		logger.Errorf("Could not get connector config for %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Get connector config failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("get connector config error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// UpdateConnectorConfig creates a new connector using the given configuration, or updates the configuration for an existing connector.
// Returns information about the connector after the change has been made. Return 409 (Conflict) if rebalance is in process.
// https://docs.confluent.io/current/connect/references/restapi.html#put--connectors-(string-name)-config
func (c *connect) UpdateConnectorConfig(req ConnectorRequest) (*ConnectorResponse, error) {
	response := new(ConnectorResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetBody(req.Config).
		SetPathParams(map[string]string{"name": req.Name}).
		Put("connectors/{name}/config")

	if err != nil {
		logger.Errorf("Could not update connector config for %v. Got error %v", req.Name, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Update connector config failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("update connector config error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// GetConnectorStatus gets current status of the connector
// including whether it is running,
// failed or paused, which worker it is assigned to,
// error information if it has failed, and the state of all its tasks.
// https://docs.confluent.io/current/connect/references/restapi.html#get--connectors-(string-name)-status
func (c *connect) GetConnectorStatus(connectorName string) (*GetConnectorStatusResponse, error) {
	response := new(GetConnectorStatusResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Get("connectors/{name}/status")

	if err != nil {
		logger.Errorf("Could not get connector status for %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Get connector status failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("get connector status error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// RestartConnector restarts the connector and its tasks. Return 409 (Conflict) if rebalance is in process.
// https://docs.confluent.io/current/connect/references/restapi.html#post--connectors-(string-name)-restart
func (c *connect) RestartConnector(connectorName string) (*EmptyResponse, error) {
	response := new(EmptyResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Post("connectors/{name}/restart")

	if err != nil {
		logger.Errorf("Could not restart connector %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Restart connector failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("restart connector error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// PauseConnector pauses the connector and its tasks, which stops message processing until the connector is resumed.
// This call asynchronous and the tasks will not transition to PAUSED state at the same time.
// https://docs.confluent.io/current/connect/references/restapi.html#put--connectors-(string-name)-pause
func (c *connect) PauseConnector(connectorName string) (*EmptyResponse, error) {
	response := new(EmptyResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Put("connectors/{name}/pause")

	if err != nil {
		logger.Errorf("Could not pause connector %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Pause connector failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("pause connector error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// ResumeConnector resumes a paused connector or do nothing if the connector is not paused.
// This call asynchronous and the tasks will not transition to RUNNING state at the same time.
// https://docs.confluent.io/current/connect/references/restapi.html#put--connectors-(string-name)-resume
func (c *connect) ResumeConnector(connectorName string) (*EmptyResponse, error) {
	response := new(EmptyResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Put("connectors/{name}/resume")

	if err != nil {
		logger.Errorf("Could not resume connector %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Resume connector failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("resume connector error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// DeleteConnector deletes a connector, halting all tasks and deleting its configuration.
// Return 409 (Conflict) if rebalance is in process.
// https://docs.confluent.io/current/connect/references/restapi.html#delete--connectors-(string-name)-
func (c *connect) DeleteConnector(connectorName string) (*EmptyResponse, error) {
	response := new(EmptyResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Delete("connectors/{name}")

	if err != nil {
		logger.Errorf("Could not delete connector %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Delete connector failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("delete connector error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// GetConnectorTasks gets a list of tasks currently running for the connector.
// https://docs.confluent.io/current/connect/references/restapi.html#get--connectors-(string-name)-tasks
func (c *connect) GetConnectorTasks(connectorName string) (*GetConnectorTasksResponse, error) {
	response := new(GetConnectorTasksResponse)
	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName}).
		Delete("connectors/{name}")

	if err != nil {
		logger.Errorf("Could not get connector tasks for connector %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("get connector tasks failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("get connector tasks error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// GetConnectorTaskStatus gets a task’s status
// https://docs.confluent.io/current/connect/references/restapi.html#get--connectors-(string-name)-tasks-(int-taskid)-status
func (c *connect) GetConnectorTaskStatus(connectorName string, taskId int) (*TaskStatusResponse, error) {
	response :=  new(TaskStatusResponse)

	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName, "task_id": strconv.Itoa(taskId)}).
		Get("connectors/{name}/tasks/{task_id}/status")
	if err != nil {
		logger.Errorf("Could not get task status for connector %v. Got error %v", connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Get connector task status failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("get connector task status error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// RestartConnectorTask restarts an individual task.
// https://docs.confluent.io/current/connect/references/restapi.html#post--connectors-(string-name)-tasks-(int-taskid)-restart
func (c *connect) RestartConnectorTask(connectorName string, taskId int) (*EmptyResponse, error) {
	response :=  new(EmptyResponse)

	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetPathParams(map[string]string{"name": connectorName, "task_id": strconv.Itoa(taskId)}).
		Get("connectors/{name}/tasks/{task_id}/restart")
	if err != nil {
		logger.Errorf("Could not get restart task with id %v for connector %v. Got error %v", taskId, connectorName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Restart connector task failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("restart connector task error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// GetConnectorPlugins returns a list of connector plugins installed in the Kafka Connect cluster.
// Note that the API only checks for connectors on the worker that handles the request,
// which means it is possible to see inconsistent results,
// especially during a rolling upgrade if you add new connector jars
// https://docs.confluent.io/current/connect/references/restapi.html#get--connector-plugins-
func (c *connect) GetConnectorPlugins()(*ConnectorPluginsResponse, error) {
	response :=  new(ConnectorPluginsResponse)

	resp, err := c.client.NewRequest().
		SetResult(&response).
		Get("connector-plugins/")
	if err != nil {
		logger.Errorf("Could not get connector plugins. Got error %v", err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Get connector plugins failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("get connector plugins error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// ValidatePluginConfig validates the provided configuration values against the configuration definition.
// This API performs per config validation, returns suggested values and error messages during validation.
// https://docs.confluent.io/current/connect/references/restapi.html#put--connector-plugins-(string-name)-config-validate
func (c *connect) ValidatePluginConfig(pluginName string, request ConnectorRequest) (*ValidateConnectorPluginResponse, error) {
	response :=  new(ValidateConnectorPluginResponse)

	resp, err := c.client.NewRequest().
		SetResult(&response).
		SetBody(request.Config).
		SetPathParams(map[string]string{"name": pluginName}).
		Put("connector-plugins/{name}/config/validate")
	if err != nil {
		logger.Errorf("Could not validate plugin %v. Got error %v", pluginName, err.Error())
		return nil, err
	}
	if resp.StatusCode() >= 400 {
		logger.Errorf("Validate plugins failed with status code: %v", resp.StatusCode())
		return nil, errors.Errorf("validate plugins error: %v", resp.String())
	}
	response.Code = resp.StatusCode()
	return response, nil
}

// CreateConnectorRequest returns a valid connector request
func (c *connect) CreateConnectorRequest(req ConnectorRequest) ConnectorRequest {
	return ConnectorRequest{
		Name:   req.Name,
		Config: req.Config,
	}
}

// creates an http client with a transporter
func createHttpClient(transport http.RoundTripper) *http.Client {
	if transport == nil {
		transport = createTransport()
	}

	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}
}

// creates a secured http transport
func createTransport() http.RoundTripper {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}
var _ Connect = (*connect)(nil)
