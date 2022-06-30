package pgcheckpoint

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
	"github.com/vmware/vmware-go-kcl/logger"
)

const (
	NumMaxRetriesPostgress = 5
)

const DefaultSchemaName = "public"
const DefaultTableNamePrefix = "kcl_"

type Connector interface {
	Connect(ctx context.Context) (*sql.DB, error)
}

type Checkpointer struct {
	log                logger.Logger
	tableName          string
	schemaName         string
	streamName         string
	leaseDuration      int
	config             *config.KinesisClientConfiguration
	retries            int
	lastLeaseSync      time.Time
	connector          Connector
	withoutCreateTable bool
}

func New(conf *Configuration) (*Checkpointer, error) {
	if conf == nil {
		return nil, errors.New("Configuration cannot be empty")
	}
	tableName := conf.TableName
	if tableName == "" {
		tableName = DefaultTableNamePrefix + conf.Client.StreamName + "_" + conf.Client.ApplicationName
	}
	// Streamname - lowercase , get rid of all alphanumeric chars / non-alphanumeric,
	if isValidTableName(tableName) {
		return nil, errors.New("Table name contains invalid characters. Allowed only letters (a-z A-Z), digits (0-9), or underscores")
	}

	schemaName := conf.SchemaName
	if schemaName == "" {
		schemaName = DefaultSchemaName
	}

	checkpointer := &Checkpointer{
		log:           conf.Client.Logger,
		tableName:     tableName,
		schemaName:    schemaName,
		streamName:    conf.Client.StreamName,
		leaseDuration: conf.Client.FailoverTimeMillis,
		config:        &conf.Client,
		retries:       NumMaxRetriesPostgress,
	}

	if checkpointer.streamName == "" {
		return nil, errors.New("StreamName cannot be null in Configuration")
	}
	return checkpointer, nil
}

func isValidTableName(e string) bool {
	tableRegex := regexp.MustCompile("[a-zA-Z0-9_]+$")
	return tableRegex.MatchString(e)
}

func Transact(db *sql.DB, txFunc func(*sql.Tx) error) (rerr error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if rerr != nil {
			_ = tx.Rollback()
		} else if err := tx.Commit(); err != nil {
			rerr = fmt.Errorf("failed to commit transaction: %w", err)
		}
	}()
	return txFunc(tx)
}

func (c *Checkpointer) getCheckpoint(shardID, streamName string) (*Checkpoint, error) {
	conn, err := c.connector.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	var ck Checkpoint
	row := conn.QueryRow(`
		SELECT shard_id, stream_name, sequence_number, lease_owner, lease_timeout, parent_id
		FROM checkpoint
		WHERE shard_id = $1
		AND stream_name = $2
	`, shardID, streamName)

	if err := row.Scan(
		&ck.ShardID, &ck.StreamName, &ck.SequenceNumber,
		&ck.LeaseOwner, &ck.LeaseTimeout, &ck.ParentID,
	); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	return &ck, nil
}

func (c *Checkpointer) saveCheckpoint(ck *Checkpoint, onlyIfUnknowned bool) error {
	conn, err := c.connector.Connect(context.TODO())
	if err != nil {
		return err
	}
	return Transact(conn, func(tx *sql.Tx) error {
		query := `
			INSERT INTO checkpoint (shard_id, stream_name, sequence_number, lease_owner, lease_timeout, parent_id)
			VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT ON CONSTRAINT shared_stream_pk
					DO UPDATE
					SET sequence_number = EXCLUDED.sequence_number,
							lease_owner = EXCLUDED.lease_owner,
							lease_timeout = EXCLUDED.lease_timeout,
							parent_id = EXCLUDED.parent_id
		`
		// FIXME:
		if onlyIfUnknowned {
			query = query + ``
			// query = query + `WHERE lease_owner IS NULL`
		}

		stmt, err := tx.Prepare(query)
		if err != nil {
			return fmt.Errorf("failed to save checkpoint: %w", err)
		}
		defer stmt.Close()

		if _, err := stmt.Exec(
			ck.ShardID, ck.StreamName, ck.SequenceNumber,
			ck.LeaseOwner, ck.LeaseTimeout, ck.ParentID,
		); err != nil {
			return fmt.Errorf("failed to save checkpoint: %w", err)
		}
		return nil
	})
}

// Fixme: Check in IHS for its usage
func (c *Checkpointer) updateCheckpoint(cp *Checkpoint, whereClause *string, whereClauseParams []string) error {
	conn, err := c.connector.Connect(context.TODO())
	if err != nil {
		return err
	}
	return Transact(conn, func(tx *sql.Tx) error {
		query := `
			UPDATE SET 
				sequence_number = $1,
				lease_owner = $2,
				lease_timeout = $3,
				parent_id = $4
		` + " " + *whereClause

		stmt, err := tx.Prepare(query)
		if err != nil {
			return errors.Wrap(err, "failed to update checkpoint")
		}
		defer stmt.Close()

		_, err = stmt.Exec(cp.SequenceNumber, cp.LeaseOwner, cp.LeaseTimeout, cp.ParentID, whereClauseParams)
		if err != nil {
			return errors.Wrap(err, "failed to save checkpoint")
		}
		return nil
	})
}

func (c *Checkpointer) removeCheckpoint(shardID, streamName string) error {
	conn, err := c.connector.Connect(context.TODO())
	if err != nil {
		return err
	}
	return Transact(conn, func(tx *sql.Tx) error {
		query := "DELETE FROM checkpoint WHERE shard_id = $1 AND stream_name = $2"

		stmt, err := tx.Prepare(query)
		if err != nil {
			return errors.Wrap(err, "failed to remove checkpoint")
		}
		defer stmt.Close()

		_, err = stmt.Exec(shardID, streamName)
		if err != nil {
			return errors.Wrap(err, "failed to remove checkpoint")
		}
		return nil
	})
}

func (c *Checkpointer) unassignCheckpointLease(shardID, streamName string) error {
	conn, err := c.connector.Connect(context.TODO())
	if err != nil {
		return err
	}
	return Transact(conn, func(tx *sql.Tx) error {
		query := "UPDATE checkpoint SET lease_owner = '' WHERE shard_id = $1 AND stream_name = $2"

		stmt, err := tx.Prepare(query)
		if err != nil {
			return errors.Wrap(err, "failed to unassign checkpoint")
		}
		defer stmt.Close()

		_, err = stmt.Exec(shardID, streamName)
		if err != nil {
			return errors.Wrap(err, "failed to unassign checkpoint")
		}
		return nil
	})
}

func (c *Checkpointer) getCheckpoints() ([]*Checkpoint, error) {
	conn, err := c.connector.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	query := `
		SELECT shard_id, stream_name, sequence_number, lease_owner, lease_timeout, parent_id FROM checkpoint`

	var rows *sql.Rows
	rows, err = conn.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get checkpoints")
	}

	defer rows.Close()
	cps := []*Checkpoint{}

	for rows.Next() {
		var cp Checkpoint

		// Fixme: handle the error
		_ = rows.Scan(&cp.ShardID, &cp.StreamName, &cp.SequenceNumber, &cp.LeaseOwner, &cp.ParentID, &cp.LeaseTimeout)
		cps = append(cps, &cp)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to load users for works: %v", err)
	}
	return cps, nil
}

func (c *Checkpointer) GetLease(shard *par.ShardStatus, assignTo string) error {
	ck, err := c.getCheckpoint(shard.ID, c.streamName)
	if err == nil {
		return nil
	}

	leaseTimeout := time.Now().Add(time.Duration(c.leaseDuration) * time.Millisecond).UTC()

	if ck.LeaseTimeout != nil && ck.LeaseOwner != "" {
		if time.Now().UTC().Before(*ck.LeaseTimeout) && ck.LeaseOwner != assignTo {
			return errors.New("unable to acquire lease: lease is already held by another node")
		}
	}

	ret := &Checkpoint{
		ShardID:      shard.ID,
		StreamName:   c.streamName,
		LeaseOwner:   assignTo,
		LeaseTimeout: &leaseTimeout,
	}

	if len(shard.ParentShardId) > 0 {
		ret.ParentID = shard.ParentShardId
	}

	if shard.Checkpoint != "" {
		ret.SequenceNumber = shard.Checkpoint
	}

	err = c.saveCheckpoint(ret, ck.LeaseTimeout == nil || ck.LeaseOwner == "")
	if err != nil {
		log.Error().Err(err).Msgf("unable to acquire lease: %s", err)
		return err
	}

	shard.Mux.Lock()
	defer shard.Mux.Unlock()
	shard.AssignedTo = assignTo
	shard.LeaseTimeout = leaseTimeout

	return nil
}

func (c *Checkpointer) CheckpointSequence(shard *par.ShardStatus) error {
	ret := &Checkpoint{
		ShardID:        shard.ID,
		StreamName:     c.streamName,
		SequenceNumber: shard.Checkpoint,
		LeaseOwner:     shard.AssignedTo,
		LeaseTimeout:   &shard.LeaseTimeout,
	}

	if len(shard.ParentShardId) > 0 {
		ret.ParentID = shard.ParentShardId
	}

	return c.saveCheckpoint(ret, false)
}

func (c *Checkpointer) FetchCheckpoint(shard *par.ShardStatus) error {
	ck, err := c.getCheckpoint(shard.ID, c.streamName)
	if err != nil {
		log.Error().Err(err).Msgf("unable to fetch checkpoint: %s", err)
		return err
	}

	if ck == nil {
		return checkpoint.ErrSequenceIDNotFound
	}

	log.Printf("Retrieved Shard Iterator %s\n", ck.SequenceNumber)

	shard.Mux.Lock()
	defer shard.Mux.Unlock()

	shard.Checkpoint = ck.SequenceNumber

	if ck.LeaseOwner != "" {
		shard.AssignedTo = ck.LeaseOwner
	}

	return nil
}

func (c *Checkpointer) RemoveLeaseInfo(shardID string) error {
	err := c.removeCheckpoint(shardID, c.streamName)
	if err != nil {
		log.Error().Err(err).Msgf("unable to remove lease info for shard: %s, stream: %s: %s", shardID, c.streamName, err)
		return err
	}

	log.Printf("Lease info for shard: %s has been removed.\n", shardID)

	return nil
}

func (c *Checkpointer) RemoveLeaseOwner(shardID string) error {
	err := c.unassignCheckpointLease(shardID, c.streamName)
	if err != nil {
		log.Error().Err(err).Msgf("unable to remove lease owner for shard: %s, stream: %s: %s", shardID, c.streamName, err)
		return err
	}

	return nil
}

// ListActiveWorkers returns a map of workers and their shards
func (checkpointer *Checkpointer) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	err := checkpointer.syncLeases(shardStatus)
	if err != nil {
		return nil, err
	}

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == checkpoint.ShardEnd {
			continue
		}

		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			log.Debug().Msgf("Shard Not Assigned Error. ShardID: %s, WorkerID: %s", shard.ID, checkpointer.config.WorkerID)
			return nil, checkpoint.ErrShardNotAssigned
		}
		if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt
func (checkpointer *Checkpointer) ClaimShard(shard *par.ShardStatus, claimID string) error {
	err := checkpointer.FetchCheckpoint(shard)
	if err != nil && err != checkpoint.ErrSequenceIDNotFound {
		return err
	}
	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339)

	i := 5
	// TODO: timestamp condition should check '=' only
	// TODO what to be done with attribute_not_exists(ClaimRequest)
	whereClause := fmt.Sprintf("WHERE shard_id = %s AND lease_timeout >= %s", nextVal(&i), nextVal(&i))
	params := []string{shard.ID, leaseTimeoutString}

	checkPointRow := &Checkpoint{
		ShardID:        shard.ID,
		StreamName:     checkpointer.streamName,
		SequenceNumber: shard.Checkpoint,
		LeaseOwner:     shard.AssignedTo,
		LeaseTimeout:   &shard.LeaseTimeout,
	}
	if leaseOwner := shard.GetLeaseOwner(); leaseOwner == "" {
		whereClause += "AND (lease_owner is null or lease_owner = '')"
	} else {
		whereClause += fmt.Sprintf("and lease_owner = %s", nextVal(&i))
		params = append(params, shard.GetLeaseOwner())
	}

	// TODO: Not sure what should be the quivalent to 'Checkpoint' as an attribute as no such columns exists
	// if checkpoint := shard.GetCheckpoint(); checkpoint == "" {
	// 	conditionalExpression += " AND attribute_not_exists(Checkpoint)"
	// } else if checkpoint == ShardEnd {
	// 	conditionalExpression += " AND Checkpoint <> :checkpoint"
	// 	expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: aws.String(ShardEnd)}
	// } else {
	// 	conditionalExpression += " AND Checkpoint = :checkpoint"
	// 	expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: &checkpoint}
	// }

	if shard.ParentShardId == "" {
		whereClause += fmt.Sprintf("and parent_id != %s", nextVal(&i))
		params = append(params, shard.ParentShardId)
	} else {
		whereClause += "AND (parent_id is null or parent_id = '')"
	}
	return checkpointer.updateCheckpoint(checkPointRow, &whereClause, params)
}

func (checkpointer *Checkpointer) syncLeases(shardStatus map[string]*par.ShardStatus) error {
	log := checkpointer.config.Logger

	if (checkpointer.lastLeaseSync.Add(time.Duration(checkpointer.config.LeaseSyncingTimeIntervalMillis) * time.Millisecond)).After(time.Now()) {
		return nil
	}

	checkpointer.lastLeaseSync = time.Now()

	results, err := checkpointer.getCheckpoints()
	if err != nil {
		log.Debugf("Error performing SyncLeases. Error: %+v ", err)
		return err
	}
	if results == nil {
		return nil
	}
	for _, result := range results {
		shardId := result.ShardID
		assignedTo := result.LeaseOwner
		checkpoint := result.SequenceNumber

		for _, shard := range shardStatus {
			if ok := (shard.ID == shardId); ok {
				shard.SetLeaseOwner(aws.StringValue(&assignedTo))
				shard.SetCheckpoint(aws.StringValue(&checkpoint))
			}
		}
	}
	log.Debugf("Lease sync completed. Next lease sync will occur in %s", time.Duration(checkpointer.config.LeaseSyncingTimeIntervalMillis)*time.Millisecond)
	return nil
}

func (c *Checkpointer) Init() error {
	if !c.withoutCreateTable {
		if err := c.CreateTable(); err != nil {
			return err
		}
	}
	return nil
}

func nextVal(i *int) string {
	*i += 1
	return fmt.Sprintf("%s%d", "$", *i)
}

func (c *Checkpointer) CreateTable() error {
	conn, err := c.connector.Connect(context.TODO())
	if err != nil {
		return err
	}

	query := `
		CREATE TABLE IF NOT EXISTS %s (
			shard_id TEXT NOT NULL,
			stream_name TEXT NOT NULL,
			sequence_number TEXT NOT NULL,
			lease_owner TEXT NOT NULL,
			lease_timeout TIMESTAMP NOT NULL,
			parent_id TEXT NOT NULL,
			CONSTRAINT shared_stream_pk PRIMARY KEY (shard_id, stream_name)
		);
	`
	stmt, err := conn.Prepare(fmt.Sprintf(query, c.tableName))
	if err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}
	return nil
}
