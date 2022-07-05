package pgcheckpoint

import "github.com/vmware/vmware-go-kcl/clientlibrary/config"

type Configuration struct {
	Client    config.KinesisClientConfiguration
	Connector Connector

	SchemaName string

	// TableName is name of the dynamo db table for managing kinesis stream default to ApplicationName
	TableName string

	WithoutCreateTable bool
}
