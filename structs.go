package connect

// FIXME: better name for this
// ConnectorPayload is the kafka connect connector registration information
type ConnectorRequest struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

//ConnectorResponse is the response returned from the connect endpoint
type ConnectorResponse struct {
	EmptyResponse
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
	Tasks  []TaskID               `json:"tasks"`
}

// EmptyResponse is response returned by kafka connect endpoints that return only the status code
type EmptyResponse struct {
	Code int
	ErrorResponse
}

// ErrorResponse is the error returned by kafka connect endpoints
type ErrorResponse struct {
	ErrorCode int    `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

//GetConnectorStatusResponse is response returned by GetStatus endpoint
type GetConnectorStatusResponse struct {
	EmptyResponse
	Name            string            `json:"name"`
	ConnectorStatus map[string]string `json:"connector"`
	TasksStatus     []TaskStatus      `json:"tasks"`
}

type GetConnectorConfigResponse struct {
	EmptyResponse
	Config map[string]interface{}
}

//GetAllConnectorsResponse is request used to get list of available connectors
type GetAllConnectorsResponse struct {
	EmptyResponse
	Connectors []string
}

type GetConnectorTasksResponse struct {
	Code  int
	Tasks []TaskDetails
}

//TaskDetails is detail of a specific task on a specific endpoint
type TaskDetails struct {
	ID     TaskID                 `json:"id"`
	Config map[string]interface{} `json:"config"`
}

//TaskID identify a task and its connector
type TaskID struct {
	Connector string `json:"connector"`
	TaskID    int    `json:"task"`
}

type TaskStatusResponse struct {
	Code   int
	Status TaskStatus
}

//TaskStatus define task status
type TaskStatus struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

type ConnectorPluginsResponse struct {
	Code  int
	Class string `json:"class"`
}

type ValidateConnectorPluginResponse struct {
	Code       int
	Name       string   `json:"name"`
	ErrorCount int      `json:"error_count"`
	Groups     []string `json:"groups"`
	Configs    Config
}

type Config struct {
	Definition map[string]interface{} `json:"definition"`
	Value      map[string]interface{} `json:"value"`
}
