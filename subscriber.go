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

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"
)

const heartbeatIntervalMillis = 10_000

type ReadResult struct {
	ChangeRecords []*ChangeRecord `spanner:"ChangeRecord" json:"change_record"`
}

type ChangeRecord struct {
	DataChangeRecords      []*DataChangeRecord      `spanner:"data_change_record" json:"data_change_record"`
	HeartbeatRecords       []*HeartbeatRecord       `spanner:"heartbeat_record" json:"heartbeat_record"`
	ChildPartitionsRecords []*ChildPartitionsRecord `spanner:"child_partitions_record" json:"child_partitions_record"`
}

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

type ColumnType struct {
	Name            string           `spanner:"name" json:"name"`
	Type            spanner.NullJSON `spanner:"type" json:"type"`
	IsPrimaryKey    bool             `spanner:"is_primary_key" json:"is_primary_key"`
	OrdinalPosition int64            `spanner:"ordinal_position" json:"ordinal_position"`
}

type Mod struct {
	Keys      spanner.NullJSON `spanner:"keys" json:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values" json:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values" json:"old_values"`
}

type HeartbeatRecord struct {
	Timestamp time.Time `spanner:"timestamp" json:"timestamp"`
}

type ChildPartitionsRecord struct {
	StartTimestamp  time.Time         `spanner:"start_timestamp" json:"start_timestamp"`
	RecordSequence  string            `spanner:"record_sequence" json:"record_sequence"`
	ChildPartitions []*ChildPartition `spanner:"child_partitions" json:"child_partitions"`
}

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

type Subscriber struct {
	client         *spanner.Client
	streamID       string
	startTimestamp time.Time
	endTimestamp   time.Time
	group          *errgroup.Group
	states         map[string]partitionState
	mu             sync.Mutex
}

func NewSubscriber(client *spanner.Client, streamID string, startTimestamp, endTimestamp time.Time) *Subscriber {
	return &Subscriber{
		client:         client,
		streamID:       streamID,
		startTimestamp: startTimestamp,
		endTimestamp:   endTimestamp,
		states:         make(map[string]partitionState),
	}
}

type Consumer interface {
	Consume(partitionToken string, result *ReadResult) error
}

func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
	group, ctx := errgroup.WithContext(ctx)
	s.group = group

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
			"heartbeat_millis_second": heartbeatIntervalMillis,
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
		var readResult ReadResult
		if err := r.ToStructLenient(&readResult); err != nil {
			return err
		}

		for _, changeRecord := range readResult.ChangeRecords {
			if len(changeRecord.ChildPartitionsRecords) > 0 {
				childPartitionRecords = append(childPartitionRecords, changeRecord.ChildPartitionsRecords...)
			}
		}

		return consumer.Consume(partitionToken, &readResult)
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
