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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	spannerpb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ReadResult is the result of the read change records from the partition.
type ReadResult struct {
	PartitionToken string          `json:"partition_token"`
	ChangeRecords  []*ChangeRecord `spanner:"ChangeRecord" json:"change_record"`
}

// ChangeRecord is the single unit of the records from the change stream.
type ChangeRecord struct {
	// Both mutable and immutable key range partition mode will return the following records.
	DataChangeRecords []*DataChangeRecord `spanner:"data_change_record" json:"data_change_record"`
	HeartbeatRecords  []*HeartbeatRecord  `spanner:"heartbeat_record" json:"heartbeat_record"`

	// Immutable key range partition mode will return the following records.
	ChildPartitionsRecords []*ChildPartitionsRecord `spanner:"child_partitions_record" json:"child_partitions_record"`

	// Mutable key range partition mode will return the following records.
	PartitionStartRecords []*PartitionStartRecord `json:"partition_start_records"`
	PartitionEndRecords   []*PartitionEndRecord   `json:"partition_end_records"`
	PartitionEventRecords []*PartitionEventRecord `json:"partition_event_records"`
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

// PartitionStartRecord contains the new partitions.
type PartitionStartRecord struct {
	StartTimestamp  time.Time `json:"start_timestamp"`
	RecordSequence  string    `json:"record_sequence"`
	PartitionTokens []string  `json:"partition_tokens"`
}

// PartitionEndRecord contains the terminated partition.
type PartitionEndRecord struct {
	EndTimestamp   time.Time `json:"end_timestamp"`
	RecordSequence string    `json:"record_sequence"`
	PartitionToken string    `json:"partition_token"`
}

// PartitionEventRecord contains key range move events between partitions.
type PartitionEventRecord struct {
	CommitTimestamp time.Time       `json:"commit_timestamp"`
	RecordSequence  string          `json:"record_sequence"`
	PartitionToken  string          `json:"partition_token"`
	MoveInEvents    []*MoveInEvent  `json:"move_in_events"`
	MoveOutEvents   []*MoveOutEvent `json:"move_out_events"`
}

// MoveInEvent contains the source partition token of the move-in event.
type MoveInEvent struct {
	SourcePartitionToken string `json:"source_partition_token"`
}

// MoveOutEvent contains the destination partition token of the move-out event.
type MoveOutEvent struct {
	DestinationPartitionToken string `json:"destination_partition_token"`
}

// changeRecordPostgres is an interim struct to decode immutable key range change stream result for PostgreSQL.
type changeRecordPostgres struct {
	DataChangeRecord      *DataChangeRecord      `spanner:"data_change_record" json:"data_change_record"`
	HeartbeatRecord       *HeartbeatRecord       `spanner:"heartbeat_record" json:"heartbeat_record"`
	ChildPartitionsRecord *ChildPartitionsRecord `spanner:"child_partitions_record" json:"child_partitions_record"`
}

type partitionState int

const (
	partitionStateUnknown partitionState = iota
	partitionStateReading
	partitionStateFinished
)

// Reader is the change stream reader.
type Reader struct {
	client            *spanner.Client
	ownsClient        bool
	streamID          string
	startTimestamp    time.Time
	endTimestamp      time.Time
	heartbeatInterval time.Duration
	dialect           dialect
	partitionMode     partitionMode
	states            map[string]partitionState
	group             *errgroup.Group
	mu                sync.Mutex
}

// Config is the configuration for the reader.
type Config struct {
	// If StartTimestamp is a zero value of time.Time, reader reads from the current timestamp.
	StartTimestamp time.Time
	// If EndTimestamp is a zero value of time.Time, reader reads until it is cancelled.
	EndTimestamp      time.Time
	HeartbeatInterval time.Duration
	// SpannerClient, if set, is reused instead of creating a new *spanner.Client.
	// The caller keeps ownership of it: Close does not close a client provided this way.
	SpannerClient        *spanner.Client
	SpannerClientConfig  spanner.ClientConfig
	SpannerClientOptions []option.ClientOption
}

// NewReader creates a new reader.
func NewReader(ctx context.Context, projectID, instanceID, databaseID, streamID string) (*Reader, error) {
	return NewReaderWithConfig(ctx, projectID, instanceID, databaseID, streamID, Config{
		SpannerClientConfig: spanner.ClientConfig{
			SessionPoolConfig: spanner.DefaultSessionPoolConfig,
		},
	})
}

// NewReaderWithConfig creates a new reader with a given configuration.
func NewReaderWithConfig(ctx context.Context, projectID, instanceID, databaseID, streamID string, config Config) (*Reader, error) {
	client := config.SpannerClient
	ownsClient := false
	if client == nil {
		dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
		var err error
		client, err = spanner.NewClientWithConfig(ctx, dbPath, config.SpannerClientConfig, config.SpannerClientOptions...)
		if err != nil {
			return nil, err
		}
		ownsClient = true
	}

	dialect, err := detectDialect(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to detect dialect: %w", err)
	}

	partitionMode, err := detectPartitionMode(ctx, client, dialect, streamID)
	if err != nil {
		return nil, fmt.Errorf("failed to detect partition mode: %w", err)
	}

	heartbeatInterval := config.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 10 * time.Second
	}

	return &Reader{
		client:            client,
		ownsClient:        ownsClient,
		streamID:          streamID,
		startTimestamp:    config.StartTimestamp,
		endTimestamp:      config.EndTimestamp,
		heartbeatInterval: heartbeatInterval,
		dialect:           dialect,
		partitionMode:     partitionMode,
		states:            make(map[string]partitionState),
	}, nil
}

// Close closes the reader.
//
// If the reader was created with an externally provided Config.SpannerClient,
// Close does not close that client — the caller retains ownership of it.
func (r *Reader) Close() {
	if r.ownsClient {
		r.client.Close()
	}
}

// Read starts reading the change stream.
//
// If function f returns an error, Read finishes the process and returns the error.
// Once this method is called, reader must not be reused in any other places (i.e. not reentrant).
func (r *Reader) Read(ctx context.Context, f func(result *ReadResult) error) error {
	r.mu.Lock()
	if r.group != nil {
		r.mu.Unlock()
		return errors.New("reader has already been read")
	}
	group, ctx := errgroup.WithContext(ctx)
	r.group = group
	r.mu.Unlock()

	r.group.Go(func() error {
		start := r.startTimestamp
		if start.IsZero() {
			start = time.Now()
		}
		return r.startRead(ctx, "", start, f)
	})

	return group.Wait()
}

func (r *Reader) startRead(ctx context.Context, partitionToken string, startTimestamp time.Time, f func(result *ReadResult) error) error {
	if !r.markStateReading(partitionToken) {
		return nil
	}
	defer r.markStateFinished(partitionToken)

	switch r.partitionMode {
	case partitionModeMutableKeyRange:
		currentStart := startTimestamp
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var stmt spanner.Statement
			currentEnd := currentStart.Add(20 * time.Minute)
			if !r.endTimestamp.IsZero() && r.endTimestamp.Before(currentEnd) {
				currentEnd = r.endTimestamp
			}

			switch r.dialect {
			case dialectGoogleSQL:
				stmt = spanner.Statement{
					SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s(@start_timestamp, @end_timestamp, @partition_token, @heartbeat_millis_second)", r.streamID),
					Params: map[string]interface{}{
						"start_timestamp":         currentStart,
						"end_timestamp":           currentEnd,
						"partition_token":         partitionToken,
						"heartbeat_millis_second": r.heartbeatInterval / time.Millisecond,
					},
				}
				if partitionToken == "" {
					stmt.Params["partition_token"] = nil
				}
			case dialectPostgreSQL:
				stmt = spanner.Statement{
					SQL: fmt.Sprintf("SELECT * FROM spanner.read_proto_bytes_%s($1, $2, $3, $4, null)", r.streamID),
					Params: map[string]interface{}{
						"p1": currentStart,
						"p2": currentEnd,
						"p3": partitionToken,
						"p4": r.heartbeatInterval / time.Millisecond,
					},
				}
				if partitionToken == "" {
					stmt.Params["p3"] = nil
				}
			default:
				return fmt.Errorf("unexpected dialect: %s", r.dialect)
			}

			var partitionStartRecords []*PartitionStartRecord
			terminated := false

			if err := r.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
				readResult := ReadResult{PartitionToken: partitionToken}
				switch r.dialect {
				case dialectGoogleSQL:
					var protoRecord spannerpb.ChangeStreamRecord
					if err := row.Column(0, &protoRecord); err != nil {
						return err
					}
					changeRecord, err := r.convertProtoRecord(&protoRecord)
					if err != nil {
						return err
					}
					if changeRecord != nil {
						readResult.ChangeRecords = []*ChangeRecord{changeRecord}
					}
				case dialectPostgreSQL:
					changeRecord, err := r.decodeProtoBytePostgresRow(row)
					if err != nil {
						return err
					}
					if changeRecord != nil {
						readResult.ChangeRecords = []*ChangeRecord{changeRecord}
					}
				default:
					return fmt.Errorf("unexpected dialect: %s", r.dialect)
				}

				for _, changeRecord := range readResult.ChangeRecords {
					if len(changeRecord.PartitionStartRecords) > 0 {
						partitionStartRecords = append(partitionStartRecords, changeRecord.PartitionStartRecords...)
					}
					if len(changeRecord.PartitionEndRecords) > 0 {
						terminated = true
					}
				}

				return f(&readResult)
			}); err != nil {
				return err
			}

			for _, p := range partitionStartRecords {
				for _, token := range p.PartitionTokens {
					childToken := token
					childStart := p.StartTimestamp
					r.group.Go(func() error {
						return r.startRead(ctx, childToken, childStart, f)
					})
				}
			}

			// Partition is terminated because received a partition end record, no need to continue reading.
			if terminated {
				break
			}

			// Stop reading if the current end timestamp is after the reader's end timestamp.
			if !r.endTimestamp.IsZero() && !currentEnd.Before(r.endTimestamp) {
				break
			}

			// Let the initial query execute once.
			if partitionToken == "" {
				break
			}

			currentStart = currentEnd
		}
		return nil

	case partitionModeImmutableKeyRange:
		var stmt spanner.Statement
		switch r.dialect {
		case dialectGoogleSQL:
			stmt = spanner.Statement{
				SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s(@start_timestamp, @end_timestamp, @partition_token, @heartbeat_millis_second)", r.streamID),
				Params: map[string]interface{}{
					"start_timestamp":         startTimestamp,
					"end_timestamp":           r.endTimestamp,
					"partition_token":         partitionToken,
					"heartbeat_millis_second": r.heartbeatInterval / time.Millisecond,
				},
			}
			if r.endTimestamp.IsZero() {
				// Must be converted to NULL.
				stmt.Params["end_timestamp"] = nil
			}
			if partitionToken == "" {
				// Must be converted to NULL.
				stmt.Params["partition_token"] = nil
			}
		case dialectPostgreSQL:
			stmt = spanner.Statement{
				SQL: fmt.Sprintf("SELECT * FROM spanner.read_json_%s($1, $2, $3, $4, null)", r.streamID),
				Params: map[string]interface{}{
					"p1": startTimestamp,
					"p2": r.endTimestamp,
					"p3": partitionToken,
					"p4": r.heartbeatInterval / time.Millisecond,
				},
			}
			if r.endTimestamp.IsZero() {
				// Must be converted to NULL.
				stmt.Params["p2"] = nil
			}
			if partitionToken == "" {
				// Must be converted to NULL.
				stmt.Params["p3"] = nil
			}
		default:
			return fmt.Errorf("unexpected dialect: %s", r.dialect)
		}

		var childPartitionRecords []*ChildPartitionsRecord
		if err := r.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
			readResult := ReadResult{PartitionToken: partitionToken}
			switch r.dialect {
			case dialectGoogleSQL:
				if err := row.ToStructLenient(&readResult); err != nil {
					return err
				}
			case dialectPostgreSQL:
				changeRecord, err := decodePostgresRow(row)
				if err != nil {
					return err
				}
				readResult.ChangeRecords = []*ChangeRecord{changeRecord}
			default:
				return fmt.Errorf("unexpected dialect: %s", r.dialect)
			}

			for _, changeRecord := range readResult.ChangeRecords {
				if len(changeRecord.ChildPartitionsRecords) > 0 {
					childPartitionRecords = append(childPartitionRecords, changeRecord.ChildPartitionsRecords...)
				}
			}

			return f(&readResult)
		}); err != nil {
			return err
		}

		r.markStateFinished(partitionToken)

		for _, childPartitionsRecord := range childPartitionRecords {
			childStartTimestamp := childPartitionsRecord.StartTimestamp
			for _, childPartition := range childPartitionsRecord.ChildPartitions {
				if r.canReadChild(childPartition) {
					partition := childPartition
					r.group.Go(func() error {
						return r.startRead(ctx, partition.Token, childStartTimestamp, f)
					})
				}
			}
		}
		return nil

	default:
		return fmt.Errorf("unexpected partition mode: %s", r.partitionMode)
	}
}

func (r *Reader) markStateReading(partitionToken string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.states[partitionToken]; ok {
		// Already started by another parent.
		return false
	}
	r.states[partitionToken] = partitionStateReading
	return true
}

func (r *Reader) markStateFinished(partitionToken string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.states[partitionToken] = partitionStateFinished
}

func (r *Reader) canReadChild(partition *ChildPartition) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, parent := range partition.ParentPartitionTokens {
		if r.states[parent] != partitionStateFinished {
			return false
		}
	}
	return true
}

func decodePostgresRow(row *spanner.Row) (*ChangeRecord, error) {
	// Retrieve JSON bytes.
	var col spanner.NullJSON
	if err := row.Column(0, &col); err != nil {
		return nil, err
	}
	jsonBytes, err := col.MarshalJSON()
	if err != nil {
		return nil, err
	}

	var changeRecordPG changeRecordPostgres
	if err := json.Unmarshal(jsonBytes, &changeRecordPG); err != nil {
		return nil, err
	}

	// Convert to ChangeRecord type.
	changeRecord := ChangeRecord{
		DataChangeRecords:      []*DataChangeRecord{},
		HeartbeatRecords:       []*HeartbeatRecord{},
		ChildPartitionsRecords: []*ChildPartitionsRecord{},
	}
	if changeRecordPG.DataChangeRecord != nil {
		changeRecord.DataChangeRecords = []*DataChangeRecord{changeRecordPG.DataChangeRecord}
	}
	if changeRecordPG.HeartbeatRecord != nil {
		changeRecord.HeartbeatRecords = []*HeartbeatRecord{changeRecordPG.HeartbeatRecord}
	}
	if changeRecordPG.ChildPartitionsRecord != nil {
		changeRecord.ChildPartitionsRecords = []*ChildPartitionsRecord{changeRecordPG.ChildPartitionsRecord}
	}

	return &changeRecord, nil
}

func (r *Reader) decodeProtoBytePostgresRow(row *spanner.Row) (*ChangeRecord, error) {
	var value []byte
	if err := row.Column(0, &value); err != nil {
		return nil, err
	}
	var protoRecord spannerpb.ChangeStreamRecord
	if err := proto.Unmarshal(value, &protoRecord); err != nil {
		return nil, err
	}
	return r.convertProtoRecord(&protoRecord)
}

func (r *Reader) convertProtoRecord(protoRecord *spannerpb.ChangeStreamRecord) (*ChangeRecord, error) {
	if protoRecord == nil {
		return nil, nil
	}

	changeRecord := &ChangeRecord{
		DataChangeRecords:     []*DataChangeRecord{},
		HeartbeatRecords:      []*HeartbeatRecord{},
		PartitionStartRecords: []*PartitionStartRecord{},
		PartitionEndRecords:   []*PartitionEndRecord{},
		PartitionEventRecords: []*PartitionEventRecord{},
	}

	switch rec := protoRecord.Record.(type) {
	case *spannerpb.ChangeStreamRecord_DataChangeRecord_:
		dcr, err := convertProtoDataChangeRecord(rec.DataChangeRecord)
		if err != nil {
			return nil, err
		}
		changeRecord.DataChangeRecords = append(changeRecord.DataChangeRecords, dcr)

	case *spannerpb.ChangeStreamRecord_HeartbeatRecord_:
		hr := &HeartbeatRecord{
			Timestamp: rec.HeartbeatRecord.Timestamp.AsTime(),
		}
		changeRecord.HeartbeatRecords = append(changeRecord.HeartbeatRecords, hr)

	case *spannerpb.ChangeStreamRecord_PartitionStartRecord_:
		psr := rec.PartitionStartRecord
		changeRecord.PartitionStartRecords = append(changeRecord.PartitionStartRecords, &PartitionStartRecord{
			StartTimestamp:  psr.StartTimestamp.AsTime(),
			RecordSequence:  psr.RecordSequence,
			PartitionTokens: psr.PartitionTokens,
		})

	case *spannerpb.ChangeStreamRecord_PartitionEndRecord_:
		per := rec.PartitionEndRecord
		changeRecord.PartitionEndRecords = append(changeRecord.PartitionEndRecords, &PartitionEndRecord{
			EndTimestamp:   per.EndTimestamp.AsTime(),
			RecordSequence: per.RecordSequence,
			PartitionToken: per.PartitionToken,
		})

	case *spannerpb.ChangeStreamRecord_PartitionEventRecord_:
		pevr := rec.PartitionEventRecord
		moveInEvents := make([]*MoveInEvent, len(pevr.MoveInEvents))
		for i, m := range pevr.MoveInEvents {
			moveInEvents[i] = &MoveInEvent{
				SourcePartitionToken: m.SourcePartitionToken,
			}
		}
		moveOutEvents := make([]*MoveOutEvent, len(pevr.MoveOutEvents))
		for i, m := range pevr.MoveOutEvents {
			moveOutEvents[i] = &MoveOutEvent{
				DestinationPartitionToken: m.DestinationPartitionToken,
			}
		}
		changeRecord.PartitionEventRecords = append(changeRecord.PartitionEventRecords, &PartitionEventRecord{
			CommitTimestamp: pevr.CommitTimestamp.AsTime(),
			RecordSequence:  pevr.RecordSequence,
			PartitionToken:  pevr.PartitionToken,
			MoveInEvents:    moveInEvents,
			MoveOutEvents:   moveOutEvents,
		})
	}

	return changeRecord, nil
}

func convertProtoDataChangeRecord(protoDcr *spannerpb.ChangeStreamRecord_DataChangeRecord) (*DataChangeRecord, error) {
	if protoDcr == nil {
		return nil, nil
	}

	dcr := &DataChangeRecord{
		CommitTimestamp:                      protoDcr.CommitTimestamp.AsTime(),
		RecordSequence:                       protoDcr.RecordSequence,
		ServerTransactionID:                  protoDcr.ServerTransactionId,
		IsLastRecordInTransactionInPartition: protoDcr.IsLastRecordInTransactionInPartition,
		TableName:                            protoDcr.Table,
		ModType:                              protoDcr.ModType.String(),
		ValueCaptureType:                     protoDcr.ValueCaptureType.String(),
		NumberOfRecordsInTransaction:         int64(protoDcr.NumberOfRecordsInTransaction),
		NumberOfPartitionsInTransaction:      int64(protoDcr.NumberOfPartitionsInTransaction),
		TransactionTag:                       protoDcr.TransactionTag,
		IsSystemTransaction:                  protoDcr.IsSystemTransaction,
	}

	columnTypes := make([]*ColumnType, len(protoDcr.ColumnMetadata))
	for i, cm := range protoDcr.ColumnMetadata {
		typeJSON, err := typeToNullJSON(cm.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert type for column %s: %w", cm.Name, err)
		}
		columnTypes[i] = &ColumnType{
			Name:            cm.Name,
			Type:            typeJSON,
			IsPrimaryKey:    cm.IsPrimaryKey,
			OrdinalPosition: cm.OrdinalPosition,
		}
	}
	dcr.ColumnTypes = columnTypes

	mods := make([]*Mod, len(protoDcr.Mods))
	for i, m := range protoDcr.Mods {
		keysJSON, err := modValuesToNullJSON(m.Keys, protoDcr.ColumnMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to convert keys: %w", err)
		}
		newValuesJSON, err := modValuesToNullJSON(m.NewValues, protoDcr.ColumnMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to convert new values: %w", err)
		}
		oldValuesJSON, err := modValuesToNullJSON(m.OldValues, protoDcr.ColumnMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to convert old values: %w", err)
		}

		mods[i] = &Mod{
			Keys:      keysJSON,
			NewValues: newValuesJSON,
			OldValues: oldValuesJSON,
		}
	}
	dcr.Mods = mods

	return dcr, nil
}

func typeToNullJSON(t *spannerpb.Type) (spanner.NullJSON, error) {
	if t == nil {
		return spanner.NullJSON{Valid: false}, nil
	}
	m := protojson.MarshalOptions{UseProtoNames: true}
	bytes, err := m.Marshal(t)
	if err != nil {
		return spanner.NullJSON{Valid: false}, err
	}
	var val interface{}
	if err := json.Unmarshal(bytes, &val); err != nil {
		return spanner.NullJSON{Valid: false}, err
	}
	return spanner.NullJSON{Value: val, Valid: true}, nil
}

func modValuesToNullJSON(modValues []*spannerpb.ChangeStreamRecord_DataChangeRecord_ModValue, columnMetadata []*spannerpb.ChangeStreamRecord_DataChangeRecord_ColumnMetadata) (spanner.NullJSON, error) {
	if len(modValues) == 0 {
		return spanner.NullJSON{Value: map[string]interface{}{}, Valid: true}, nil
	}

	resultMap := make(map[string]interface{})
	for _, mv := range modValues {
		idx := mv.ColumnMetadataIndex
		if int(idx) >= len(columnMetadata) {
			return spanner.NullJSON{Valid: false}, fmt.Errorf("column metadata index %d out of range (len=%d)", idx, len(columnMetadata))
		}
		colName := columnMetadata[idx].Name
		var val interface{}
		if mv.Value != nil {
			val = mv.Value.AsInterface()
		}
		resultMap[colName] = val
	}

	return spanner.NullJSON{Value: resultMap, Valid: true}, nil
}
