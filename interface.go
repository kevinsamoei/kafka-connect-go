package connect

type Connect interface {
	// connector
	CreateConnectorRequest(ConnectorRequest) ConnectorRequest
	GetConnectors() (*GetAllConnectorsResponse, error)
	CreateConnector(request ConnectorRequest) (*ConnectorResponse, error)
	GetConnector(connectorName string) (*ConnectorResponse, error)
	GetConnectorConfig(connectorName string) (*GetConnectorConfigResponse, error)
	UpdateConnectorConfig(request ConnectorRequest) (*ConnectorResponse, error)
	GetConnectorStatus(connectorName string) (*GetConnectorStatusResponse, error)
	RestartConnector(connectorName string) (*EmptyResponse, error)
	PauseConnector(connectorName string) (*EmptyResponse, error)
	ResumeConnector(connectorName string) (*EmptyResponse, error)
	DeleteConnector(connectorName string) (*EmptyResponse, error)

	// Tasks
	GetConnectorTasks(connectorName string) (*GetConnectorTasksResponse, error)
	GetConnectorTaskStatus(connectorName string, taskId int) (*TaskStatusResponse, error)
	RestartConnectorTask(connectorName string, taskId int) (*EmptyResponse, error)

	// plugins
	GetConnectorPlugins() (*ConnectorPluginsResponse, error)
	ValidatePluginConfig(pluginName string, request ConnectorRequest) (*ValidateConnectorPluginResponse, error)
}
