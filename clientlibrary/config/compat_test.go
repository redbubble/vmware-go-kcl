package config

import (
	"testing"

	creds "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	"github.com/vmware/vmware-go-kcl/logger"
)

func TestConfigurationCompatibility(t *testing.T) {
	compat := KinesisClientLibConfiguration{
		ApplicationName:                                  "application",
		KinesisEndpoint:                                  "kinesis.us-west-1.amazonaws.com",
		KinesisCredentials:                               &creds.Credentials{},
		StreamName:                                       "arn:aws:kinesis:us-east-1:12345678:stream/some-stream",
		EnableEnhancedFanOutConsumer:                     true,
		EnhancedFanOutConsumerName:                       "foobar",
		EnhancedFanOutConsumerARN:                        "arn:aws:kinesis:us-east-1:123456789123:stream/foobar/consumer/test-consumer:1525898737",
		WorkerID:                                         "2345678",
		InitialPositionInStream:                          1,
		InitialPositionInStreamExtended:                  InitialPositionInStreamExtended{Position: 1},
		FailoverTimeMillis:                               10000,
		LeaseRefreshPeriodMillis:                         100000,
		MaxRecords:                                       500,
		IdleTimeBetweenReadsInMillis:                     1000,
		CallProcessRecordsEvenForEmptyRecordList:         true,
		ParentShardPollIntervalMillis:                    100,
		ShardSyncIntervalMillis:                          100,
		CleanupTerminatedShardsBeforeExpiry:              true,
		TaskBackoffTimeMillis:                            100,
		ValidateSequenceNumberBeforeCheckpointing:        true,
		RegionName:                                       "us-east-1",
		ShutdownGraceMillis:                              100,
		MaxLeasesForWorker:                               100,
		MaxLeasesToStealAtOneTime:                        10,
		SkipShardSyncAtWorkerInitializationIfLeasesExist: true,
		Logger:                          logger.GetDefaultLogger(),
		MonitoringService:               metrics.NoopMonitoringService{},
		EnableLeaseStealing:             true,
		LeaseStealingIntervalMillis:     1000,
		LeaseStealingClaimTimeoutMillis: 2000,
		LeaseSyncingTimeIntervalMillis:  2000,
		TableName:                       "checkpoint",
		InitialLeaseTableReadCapacity:   4,
		InitialLeaseTableWriteCapacity:  1,
		DynamoDBEndpoint:                "dynamodb.us-east-1.amazonaws.com",
		DynamoDBCredentials:             &creds.Credentials{},
	}

	t.Run("base", func(t *testing.T) {
		kinesis := compat.KinesisClientConfiguration()
		expected := KinesisClientConfiguration{
			ApplicationName:                                  "application",
			KinesisEndpoint:                                  "kinesis.us-west-1.amazonaws.com",
			KinesisCredentials:                               &creds.Credentials{},
			StreamName:                                       "arn:aws:kinesis:us-east-1:12345678:stream/some-stream",
			EnableEnhancedFanOutConsumer:                     true,
			EnhancedFanOutConsumerName:                       "foobar",
			EnhancedFanOutConsumerARN:                        "arn:aws:kinesis:us-east-1:123456789123:stream/foobar/consumer/test-consumer:1525898737",
			WorkerID:                                         "2345678",
			InitialPositionInStream:                          1,
			InitialPositionInStreamExtended:                  InitialPositionInStreamExtended{Position: 1},
			FailoverTimeMillis:                               10000,
			LeaseRefreshPeriodMillis:                         100000,
			MaxRecords:                                       500,
			IdleTimeBetweenReadsInMillis:                     1000,
			CallProcessRecordsEvenForEmptyRecordList:         true,
			ParentShardPollIntervalMillis:                    100,
			ShardSyncIntervalMillis:                          100,
			CleanupTerminatedShardsBeforeExpiry:              true,
			TaskBackoffTimeMillis:                            100,
			ValidateSequenceNumberBeforeCheckpointing:        true,
			RegionName:                                       "us-east-1",
			ShutdownGraceMillis:                              100,
			MaxLeasesForWorker:                               100,
			MaxLeasesToStealAtOneTime:                        10,
			SkipShardSyncAtWorkerInitializationIfLeasesExist: true,
			Logger:                          logger.GetDefaultLogger(),
			MonitoringService:               metrics.NoopMonitoringService{},
			EnableLeaseStealing:             true,
			LeaseStealingIntervalMillis:     1000,
			LeaseStealingClaimTimeoutMillis: 2000,
			LeaseSyncingTimeIntervalMillis:  2000}

		assert.Equal(t, expected, kinesis)
	})

	t.Run("dynamo", func(t *testing.T) {
		dynamo := compat.DynamoCheckpointerConfiguration()
		expected := DynamoCheckpointerConfiguration{
			TableName:                      "checkpoint",
			InitialLeaseTableReadCapacity:  4,
			InitialLeaseTableWriteCapacity: 1,
			DynamoDBEndpoint:               "dynamodb.us-east-1.amazonaws.com",
			DynamoDBCredentials:            &creds.Credentials{},
		}
		assert.Equal(t, expected, dynamo)
	})
}
