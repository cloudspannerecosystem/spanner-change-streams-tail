//
// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package changestreams

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"
)

// ReadResult is the result of the read change records from the partition.
type ReadResult struct {
	PartitionToken string          `json:"partition_token"`
	ChangeRecords  []*ChangeRecord `spanner:"ChangeRecord" json:"change_record"`
}

// ChangeRecord is the single unit of the records from the change stream.
type ChangeRecord struct {
	DataChangeRecords      []*DataChangeRecord      `spanner:"data_change_record" json:"data_change_record"`
	HeartbeatRecords       []*HeartbeatRecord       `spanner:"heartbeat_record" json:"heartbeat_record"`
	ChildPartitionsRecords []*ChildPartitionsRecord `spanner:"child_partitions_record" json:"child_partitions_record"`
}

// DataChangeRecord contains a set of changes to the table.
type DataChangeRecord struct {
	CommitTimestamp                      time.Time     `spanner:"commit_timestamp" json:"commit_timestamp"`
	RecordSequence                       string        `spanner:"record_sequence" json:"record_sequence"`
	ServerTransactionID                  string        `spanner:"server_transaction_id" json:"server_transaction_id"`
	IsLastRecordInTransactionInPartition bool          `spanner:"is_last_record_in_transaction_in_partition" json:"is_last_record_in_transaction_in_partition"`
	TableName                            string        `spanner:"table_name" json:"table_name"`
	ColumnTypes                          []*ColumnType `spanner:"column_types" json:"column_types"`
	Mods                                 []*Mod        `spanner:"mods" json:"mods"`
	ModType                              string        `spanner:"mod_type" json:"mod_type"`
	ValueCaptureType                     string        `spanner:"value_capture_type" json:"value_capture_type"`
	NumberOfRecordsInTransaction         int64         `spanner:"number_of_records_in_transaction" json:"number_of_records_in_transaction"`
	NumberOfPartitionsInTransaction      int64         `spanner:"number_of_partitions_in_transaction" json:"number_of_partitions_in_transaction"`
	TransactionTag                       string        `spanner:"transaction_tag" json:"transaction_tag"`
	IsSystemTransaction                  bool          `spanner:"is_system_transaction" json:"is_system_transaction"`
}

// ColumnType is the metadata of the column.
type ColumnType struct {
	Name            string           `spanner:"name" json:"name"`
	Type            spanner.NullJSON `spanner:"type" json:"type"`
	IsPrimaryKey    bool             `spanner:"is_primary_key" json:"is_primary_key"`
	OrdinalPosition int64            `spanner:"ordinal_position" json:"ordinal_position"`
}

// Mod is the changes that were made on the table.
type Mod struct {
	Keys      spanner.NullJSON `spanner:"keys" json:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values" json:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values" json:"old_values"`
}

// HeartbeatRecord is the heartbeat record returned from Cloud Spanner.
type HeartbeatRecord struct {
	Timestamp time.Time `spanner:"timestamp" json:"timestamp"`
}

// ChildPartitionsRecord contains the child partitions of the stream.
type ChildPartitionsRecord struct {
	StartTimestamp  time.Time         `spanner:"start_timestamp" json:"start_timestamp"`
	RecordSequence  string            `spanner:"record_sequence" json:"record_sequence"`
	ChildPartitions []*ChildPartition `spanner:"child_partitions" json:"child_partitions"`
}

// ChildPartition contains the child partition token.
type ChildPartition struct {
	Token                 string   `spanner:"token" json:"token"`
	ParentPartitionTokens []string `spanner:"parent_partition_tokens" json:"parent_partition_tokens"`
}

type partitionState int

const (
	partitionStateUnknown partitionState = iota
	partitionStateReading
	partitionStateFinished
)

// Subscriber is the change stream subscriber.
type Subscriber struct {
	client            *spanner.Client
	streamID          string
	startTimestamp    time.Time
	endTimestamp      time.Time
	heartbeatInterval time.Duration
	states            map[string]partitionState
	group             *errgroup.Group
	mu                sync.Mutex
}

// Config is the configuration for the subscriber.
type Config struct {
	// If StartTimestamp is a zero value of time.Time, subscriber subscribes from the current timestamp.
	StartTimestamp time.Time
	// If EndTimestamp is a zero value of time.Time, subscriber subscribes until it is cancelled.
	EndTimestamp      time.Time
	HeartbeatInterval time.Duration
}

// NewSubscriber creates a new subscriber.
func NewSubscriber(ctx context.Context, projectID, instanceID, databaseID, streamID string) (*Subscriber, error) {
	return NewSubscriberWithConfig(ctx, projectID, instanceID, databaseID, streamID, &Config{})
}

// NewSubscriberWithConfig creates a new subscriber with the given configuration.
func NewSubscriberWithConfig(ctx context.Context, projectID, instanceID, databaseID, streamID string, config *Config) (*Subscriber, error) {
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewClientWithConfig(ctx, dbPath, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			WriteSessions: 0,
		},
	})
	if err != nil {
		return nil, err
	}

	heartbeatInterval := config.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 10 * time.Second
	}

	return &Subscriber{
		client:            client,
		streamID:          streamID,
		startTimestamp:    config.StartTimestamp,
		endTimestamp:      config.EndTimestamp,
		heartbeatInterval: heartbeatInterval,
		states:            make(map[string]partitionState),
	}, nil
}

// Close closes the subscriber.
func (s *Subscriber) Close() {
	s.client.Close()
}

// Consumer is the interface to consume the read results from the change stream.
//
// Consume could be called from multiple goroutines, so it must be reentrant-safe.
type Consumer interface {
	Consume(result *ReadResult) error
}

// ConsumerFunc type is an adapter to allow the use of ordinary functions as Consumer.
type ConsumerFunc func(*ReadResult) error

// Consume calls f(result).
func (f ConsumerFunc) Consume(result *ReadResult) error {
	return f(result)
}

// Subscribe starts subscribing the change stream.
//
// If consumer returns an error, Subscribe finishes the process and returns the error.
// Once this method is called, subscriber must not be reused in any other places (i.e. not reentrant).
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
	s.mu.Lock()
	if s.group != nil {
		s.mu.Unlock()
		return errors.New("subscriber has already been subscribed")
	}
	group, ctx := errgroup.WithContext(ctx)
	s.group = group
	s.mu.Unlock()

	s.group.Go(func() error {
		start := s.startTimestamp
		if start.IsZero() {
			start = time.Now()
		}
		return s.startRead(ctx, "", start, consumer)
	})

	return group.Wait()
}

func (s *Subscriber) startRead(ctx context.Context, partitionToken string, startTimestamp time.Time, consumer Consumer) error {
	if !s.markStateReading(partitionToken) {
		return nil
	}

	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s(@start_timestamp, @end_timestamp, @partition_token, @heartbeat_millis_second)", s.streamID),
		Params: map[string]interface{}{
			"start_timestamp":         startTimestamp,
			"end_timestamp":           s.endTimestamp,
			"partition_token":         partitionToken,
			"heartbeat_millis_second": s.heartbeatInterval / time.Millisecond,
		},
	}
	if s.endTimestamp.IsZero() {
		// Must be converted to NULL.
		stmt.Params["end_timestamp"] = nil
	}
	if partitionToken == "" {
		// Must be converted to NULL.
		stmt.Params["partition_token"] = nil
	}

	var childPartitionRecords []*ChildPartitionsRecord
	if err := s.client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		readResult := ReadResult{PartitionToken: partitionToken}
		if err := r.ToStructLenient(&readResult); err != nil {
			return err
		}

		for _, changeRecord := range readResult.ChangeRecords {
			if len(changeRecord.ChildPartitionsRecords) > 0 {
				childPartitionRecords = append(childPartitionRecords, changeRecord.ChildPartitionsRecords...)
			}
		}

		return consumer.Consume(&readResult)
	}); err != nil {
		return err
	}

	s.markStateFinished(partitionToken)

	for _, childPartitionsRecord := range childPartitionRecords {
		// childStartTimestamp is always later than s.startTimestamp.
		childStartTimestamp := childPartitionsRecord.StartTimestamp
		for _, childPartition := range childPartitionsRecord.ChildPartitions {
			if s.canReadChild(childPartition) {
				s.group.Go(func() error {
					return s.startRead(ctx, childPartition.Token, childStartTimestamp, consumer)
				})
			}
		}
	}

	return nil
}

func (s *Subscriber) markStateReading(partitionToken string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.states[partitionToken]; ok {
		// Already started by another parent.
		return false
	}
	s.states[partitionToken] = partitionStateReading
	return true
}

func (s *Subscriber) markStateFinished(partitionToken string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.states[partitionToken] = partitionStateFinished
}

func (s *Subscriber) canReadChild(partition *ChildPartition) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, parent := range partition.ParentPartitionTokens {
		if s.states[parent] != partitionStateFinished {
			return false
		}
	}
	return true
}
