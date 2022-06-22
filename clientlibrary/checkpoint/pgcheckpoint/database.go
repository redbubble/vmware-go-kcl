package pgcheckpoint

import (
	"context"
	"database/sql"
)

type Datastore interface {
	ServiceName() string
	GetDBStats() sql.DBStats
	PingContext(context.Context) error
	Close() error
}

type ConsumerDatastore interface {
	Datastore
	GetCheckpoint(shardID, streamName string) (*Checkpoint, error)
	SaveCheckpoint(*Checkpoint, bool) error
	RemoveCheckpoint(shardID, streamName string) error
	UnassignCheckpointLease(shardID, streamName string) error
	GetCheckpoints() ([]*Checkpoint, error)
	UpdateCheckpoint(cp *Checkpoint, whereClause *string, whereClauseParams []string) error
}
