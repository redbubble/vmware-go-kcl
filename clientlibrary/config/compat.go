package config

import (
	creds "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	"github.com/vmware/vmware-go-kcl/logger"
)

// Deprecated: Use KinesisClientConfiguration and the appropriate Checkpoint Config (i.e. DynamoCheckpointerConfiguration)
// Configuration for the Kinesis Client Library.
// Note: There is no need to configure credential provider. Credential can be get from InstanceProfile.
type KinesisClientLibConfiguration struct {
	// ApplicationName is name of application. Kinesis allows multiple applications to consume the same stream.
	ApplicationName string

	// DynamoDBEndpoint is an optional endpoint URL that overrides the default generated endpoint for a DynamoDB client.
	// If this is empty, the default generated endpoint will be used.
	DynamoDBEndpoint string

	// KinesisEndpoint is an optional endpoint URL that overrides the default generated endpoint for a Kinesis client.
	// If this is empty, the default generated endpoint will be used.
	KinesisEndpoint string

	// KinesisCredentials is used to access Kinesis
	KinesisCredentials *creds.Credentials

	// DynamoDBCredentials is used to access DynamoDB
	DynamoDBCredentials *creds.Credentials

	// TableName is name of the dynamo db table for managing kinesis stream default to ApplicationName
	TableName string

	// StreamName is the name of Kinesis stream
	StreamName string

	// EnableEnhancedFanOutConsumer enables enhanced fan-out consumer
	// See: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
	// Either consumer name or consumer ARN must be specified when Enhanced Fan-Out is enabled.
	EnableEnhancedFanOutConsumer bool

	// EnhancedFanOutConsumerName is the name of the enhanced fan-out consumer to create. If this isn't set the ApplicationName will be used.
	EnhancedFanOutConsumerName string

	// EnhancedFanOutConsumerARN is the ARN of an already created enhanced fan-out consumer, if this is set no automatic consumer creation will be attempted
	EnhancedFanOutConsumerARN string

	// WorkerID used to distinguish different workers/processes of a Kinesis application
	WorkerID string

	// InitialPositionInStream specifies the Position in the stream where a new application should start from
	InitialPositionInStream InitialPositionInStream

	// InitialPositionInStreamExtended provides actual AT_TIMESTAMP value
	InitialPositionInStreamExtended InitialPositionInStreamExtended

	// credentials to access Kinesis/Dynamo: https://docs.aws.amazon.com/sdk-for-go/api/aws/credentials/
	// Note: No need to configure here. Use NewEnvCredentials for testing and EC2RoleProvider for production

	// FailoverTimeMillis Lease duration (leases not renewed within this period will be claimed by others)
	FailoverTimeMillis int

	// LeaseRefreshPeriodMillis is the period before the end of lease during which a lease is refreshed by the owner.
	LeaseRefreshPeriodMillis int

	// MaxRecords Max records to read per Kinesis getRecords() call
	MaxRecords int

	// IdleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
	IdleTimeBetweenReadsInMillis int

	// CallProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
	// GetRecords returned an empty record list.
	CallProcessRecordsEvenForEmptyRecordList bool

	// ParentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done
	ParentShardPollIntervalMillis int

	// ShardSyncIntervalMillis Time between tasks to sync leases and Kinesis shards
	ShardSyncIntervalMillis int

	// CleanupTerminatedShardsBeforeExpiry Clean up shards we've finished processing (don't wait for expiration)
	CleanupTerminatedShardsBeforeExpiry bool

	// kinesisClientConfig Client Configuration used by Kinesis client
	// dynamoDBClientConfig Client Configuration used by DynamoDB client
	// Note: we will use default client provided by AWS SDK

	// TaskBackoffTimeMillis Backoff period when tasks encounter an exception
	TaskBackoffTimeMillis int

	// ValidateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
	ValidateSequenceNumberBeforeCheckpointing bool

	// RegionName The region name for the service
	RegionName string

	// ShutdownGraceMillis The number of milliseconds before graceful shutdown terminates forcefully
	ShutdownGraceMillis int

	// Operation parameters

	// Max leases this Worker can handle at a time
	MaxLeasesForWorker int

	// Max leases to steal at one time (for load balancing)
	MaxLeasesToStealAtOneTime int

	// Read capacity to provision when creating the lease table (dynamoDB).
	InitialLeaseTableReadCapacity int

	// Write capacity to provision when creating the lease table.
	InitialLeaseTableWriteCapacity int

	// Worker should skip syncing shards and leases at startup if leases are present
	// This is useful for optimizing deployments to large fleets working on a stable stream.
	SkipShardSyncAtWorkerInitializationIfLeasesExist bool

	// Logger used to log message.
	Logger logger.Logger

	// MonitoringService publishes per worker-scoped metrics.
	MonitoringService metrics.MonitoringService

	// EnableLeaseStealing turns on lease stealing
	EnableLeaseStealing bool

	// LeaseStealingIntervalMillis The number of milliseconds between rebalance tasks
	LeaseStealingIntervalMillis int

	// LeaseStealingClaimTimeoutMillis The number of milliseconds to wait before another worker can aquire a claimed shard
	LeaseStealingClaimTimeoutMillis int

	// LeaseSyncingTimeInterval The number of milliseconds to wait before syncing with lease table (dynamoDB)
	LeaseSyncingTimeIntervalMillis int
}

func (c KinesisClientLibConfiguration) KinesisClientConfiguration() KinesisClientConfiguration {
	return KinesisClientConfiguration{
		ApplicationName:                                  c.ApplicationName,
		KinesisEndpoint:                                  c.KinesisEndpoint,
		KinesisCredentials:                               c.KinesisCredentials,
		StreamName:                                       c.StreamName,
		EnableEnhancedFanOutConsumer:                     c.EnableEnhancedFanOutConsumer,
		EnhancedFanOutConsumerName:                       c.EnhancedFanOutConsumerName,
		EnhancedFanOutConsumerARN:                        c.EnhancedFanOutConsumerARN,
		WorkerID:                                         c.WorkerID,
		InitialPositionInStream:                          c.InitialPositionInStream,
		InitialPositionInStreamExtended:                  c.InitialPositionInStreamExtended,
		FailoverTimeMillis:                               c.FailoverTimeMillis,
		LeaseRefreshPeriodMillis:                         c.LeaseRefreshPeriodMillis,
		MaxRecords:                                       c.MaxRecords,
		IdleTimeBetweenReadsInMillis:                     c.IdleTimeBetweenReadsInMillis,
		CallProcessRecordsEvenForEmptyRecordList:         c.CallProcessRecordsEvenForEmptyRecordList,
		ParentShardPollIntervalMillis:                    c.ParentShardPollIntervalMillis,
		ShardSyncIntervalMillis:                          c.ShardSyncIntervalMillis,
		CleanupTerminatedShardsBeforeExpiry:              c.CleanupTerminatedShardsBeforeExpiry,
		TaskBackoffTimeMillis:                            c.TaskBackoffTimeMillis,
		ValidateSequenceNumberBeforeCheckpointing:        c.ValidateSequenceNumberBeforeCheckpointing,
		RegionName:                                       c.RegionName,
		ShutdownGraceMillis:                              c.ShutdownGraceMillis,
		MaxLeasesForWorker:                               c.MaxLeasesForWorker,
		MaxLeasesToStealAtOneTime:                        c.MaxLeasesToStealAtOneTime,
		SkipShardSyncAtWorkerInitializationIfLeasesExist: c.SkipShardSyncAtWorkerInitializationIfLeasesExist,
		Logger:                          c.Logger,
		MonitoringService:               c.MonitoringService,
		EnableLeaseStealing:             c.EnableLeaseStealing,
		LeaseStealingIntervalMillis:     c.LeaseStealingIntervalMillis,
		LeaseStealingClaimTimeoutMillis: c.LeaseStealingClaimTimeoutMillis,
		LeaseSyncingTimeIntervalMillis:  c.LeaseSyncingTimeIntervalMillis,
	}
}

func (c KinesisClientLibConfiguration) DynamoCheckpointerConfiguration() DynamoCheckpointerConfiguration {
	return DynamoCheckpointerConfiguration{
		TableName:                      c.TableName,
		InitialLeaseTableReadCapacity:  c.InitialLeaseTableReadCapacity,
		InitialLeaseTableWriteCapacity: c.InitialLeaseTableWriteCapacity,
		DynamoDBEndpoint:               c.DynamoDBEndpoint,
		DynamoDBCredentials:            c.DynamoDBCredentials,
	}
}
