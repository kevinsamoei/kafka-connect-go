package connect

// FIXME: Tests are yet to be implemented

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type ConnectTestSuite struct {
	suite.Suite
}

func TestConnectTestSuite(t *testing.T)  {
	suite.Run(t, new(ConnectTestSuite))
}

func (suite *ConnectTestSuite) SetupTest()  {}

func (suite *ConnectTestSuite) TearDownTest()  {}

func (suite *ConnectTestSuite) TestCreateConnectorRequest() {
	suite.Require().Equal(nil, nil)
}
func(suite *ConnectTestSuite) TestGetConnectors() {
	suite.T().Log("Testing get all connectors")
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) CreateConnector() {
	suite.T().Log("Testing Create connectors")
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) GetConnector() {
	suite.T().Log("Testing get single connectors")
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) GetConnectorConfig() {
	suite.T().Log("Testing get connector config")
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) UpdateConnectorConfig(){
	suite.T().Log("Testing update connector config")
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) GetConnectorStatus(){
	suite.T().Log("Testing get connector status")
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) RestartConnector(){
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite)PauseConnector(){
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite)ResumeConnector(){
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) DeleteConnector(){
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) GetConnectorTasks(){
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) GetConnectorTaskStatus(){
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) RestartConnectorTask(){
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) GetConnectorPlugins() {
	suite.Require().Equal(nil, nil)
}
func (suite *ConnectTestSuite) ValidatePluginConfig() {
	suite.Require().Equal(nil, nil)
}
