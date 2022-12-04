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
	"os"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/option"

	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	envTestProjectID  = "TEST_PROJECT_ID"
	envTestInstanceID = "TEST_INSTANCE_ID"
	envTestDatabaseID = "TEST_DATABASE_ID"
	envTestCredential = "TEST_CREDENTIAL"
	timeoutPerTest    = time.Minute * 3
)

var (
	skipIntegrateTest bool

	testProjectID  string
	testInstanceID string
	testDatabaseID string
	testCredential string

	tableIDCounter  uint32
	streamIDCounter uint32
)

func TestMain(m *testing.M) {
	initialize()
	os.Exit(m.Run())
}

func initialize() {
	if os.Getenv(envTestProjectID) == "" || os.Getenv(envTestInstanceID) == "" || os.Getenv(envTestDatabaseID) == "" {
		skipIntegrateTest = true
		return
	}

	testProjectID = os.Getenv(envTestProjectID)
	testInstanceID = os.Getenv(envTestInstanceID)
	testDatabaseID = os.Getenv(envTestDatabaseID)
	testCredential = os.Getenv(envTestCredential)
}

func generateUniqueTableID() string {
	count := atomic.AddUint32(&tableIDCounter, 1)
	return fmt.Sprintf("table_%d_%d", time.Now().Unix(), count)
}

func generateUniqueStreamID() string {
	count := atomic.AddUint32(&streamIDCounter, 1)
	return fmt.Sprintf("stream_%d_%d", time.Now().Unix(), count)
}

type setupResult struct {
	client   *spanner.Client
	tableID  string
	streamID string
	tearDown func() error
}

func setup(ctx context.Context, t *testing.T) (*setupResult, error) {
	var options []option.ClientOption
	if testCredential != "" {
		options = append(options, option.WithCredentialsJSON([]byte(testCredential)))
	}
	adminClient, err := adminapi.NewDatabaseAdminClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create an admin client: %v", err)
	}

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectID, testInstanceID, testDatabaseID)
	client, err := spanner.NewClient(ctx, dbPath, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create a client: %v", err)
	}

	tableID := generateUniqueTableID()
	tableDDL := fmt.Sprintf(`
	CREATE TABLE %s (
	  id INT64 NOT NULL,
	  active BOOL NOT NULL
	) PRIMARY KEY (id)
	`, tableID)

	streamID := generateUniqueStreamID()
	streamDDL := fmt.Sprintf("CREATE CHANGE STREAM %s FOR %s", streamID, tableID)

	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: []string{tableDDL, streamDDL},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update database DDL: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		return nil, fmt.Errorf("failed to update database DDL: %v", err)
	}
	t.Logf("Created table %q and stream %q", tableID, streamID)

	tearDown := func() error {
		op, err = adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database: dbPath,
			Statements: []string{
				// Change stream must be dropped first before dropping a watched table.
				// Otherwise, FailedPrecondition error happens.
				fmt.Sprintf("DROP CHANGE STREAM %s", streamID),
				fmt.Sprintf("DROP TABLE %s", tableID),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to update database DDL: %v", err)
		}
		if err := op.Wait(ctx); err != nil {
			return fmt.Errorf("failed to update database DDL: %v", err)
		}
		t.Logf("Deleted table %q and stream %q", tableID, streamID)
		return nil
	}

	return &setupResult{
		client:   client,
		tearDown: tearDown,
		tableID:  tableID,
		streamID: streamID,
	}, nil
}

type dataChangeRecordConsumer struct {
	records []*DataChangeRecord
}

func (c *dataChangeRecordConsumer) Consume(partitionToken string, result *ReadResult) error {
	for _, changeRecord := range result.ChangeRecords {
		for _, r := range changeRecord.DataChangeRecords {
			c.records = append(c.records, r)
		}
	}
	return nil
}

func TestSubscriber(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutPerTest)
	defer cancel()

	setupResult, err := setup(ctx, t)
	if err != nil {
		t.Fatalf("failed to setup: %v", err)
	}
	defer func() {
		if err := setupResult.tearDown(); err != nil {
			t.Fatalf("failed to tear down: %v", err)
		}
	}()

	for _, test := range []struct {
		desc     string
		dmls     []string
		expected []*DataChangeRecord
	}{
		{
			desc: "data change record",
			dmls: []string{
				fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true)", setupResult.tableID),
				fmt.Sprintf("DELETE FROM %s WHERE id = 1", setupResult.tableID),
			},
			expected: []*DataChangeRecord{
				{
					RecordSequence:                       "00000000",
					IsLastRecordInTransactionInPartition: false,
					TableName:                            setupResult.tableID,
					ColumnTypes: []*ColumnType{
						{
							Name: "id",
							Type: spanner.NullJSON{
								Value: map[string]interface{}{"code": "INT64"},
								Valid: true,
							},
							IsPrimaryKey:    true,
							OrdinalPosition: 1,
						},
						{
							Name: "active",
							Type: spanner.NullJSON{
								Value: map[string]interface{}{"code": "BOOL"},
								Valid: true,
							},
							IsPrimaryKey:    false,
							OrdinalPosition: 2,
						},
					},
					Mods: []*Mod{
						{
							Keys: spanner.NullJSON{
								Value: map[string]interface{}{"id": "1"},
								Valid: true,
							},
							NewValues: spanner.NullJSON{
								Value: map[string]interface{}{"active": true},
								Valid: true,
							},
							OldValues: spanner.NullJSON{
								Value: map[string]interface{}{},
								Valid: true,
							},
						},
					},
					ModType:                         "INSERT",
					ValueCaptureType:                "OLD_AND_NEW_VALUES",
					NumberOfRecordsInTransaction:    2,
					NumberOfPartitionsInTransaction: 1,
					TransactionTag:                  "",
					IsSystemTransaction:             false,
				},
				{
					RecordSequence:                       "00000001",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            setupResult.tableID,
					ColumnTypes: []*ColumnType{
						{
							Name: "id",
							Type: spanner.NullJSON{
								Value: map[string]interface{}{"code": "INT64"},
								Valid: true,
							},
							IsPrimaryKey:    true,
							OrdinalPosition: 1,
						},
						{
							Name: "active",
							Type: spanner.NullJSON{
								Value: map[string]interface{}{"code": "BOOL"},
								Valid: true,
							},
							IsPrimaryKey:    false,
							OrdinalPosition: 2,
						},
					},
					Mods: []*Mod{
						{
							Keys: spanner.NullJSON{
								Value: map[string]interface{}{"id": "1"},
								Valid: true,
							},
							NewValues: spanner.NullJSON{
								Value: map[string]interface{}{},
								Valid: true,
							},
							OldValues: spanner.NullJSON{
								Value: map[string]interface{}{"active": true},
								Valid: true,
							},
						},
					},
					ModType:                         "DELETE",
					ValueCaptureType:                "OLD_AND_NEW_VALUES",
					NumberOfRecordsInTransaction:    2,
					NumberOfPartitionsInTransaction: 1,
					TransactionTag:                  "",
					IsSystemTransaction:             false,
				},
			},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			client := setupResult.client
			var startTimestamp, endTimestamp time.Time
			subscriber := NewSubscriber(client, setupResult.streamID, startTimestamp, endTimestamp)

			consumer := &dataChangeRecordConsumer{}
			subscriberContext, subscriberCancel := context.WithCancel(ctx)
			go subscriber.Subscribe(subscriberContext, consumer)

			if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				for _, dml := range test.dmls {
					if _, err := txn.Update(ctx, spanner.NewStatement(dml)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("failed to add test data: %v", err)
			}

			// Wait a bit and stop subscriber.
			time.Sleep(5 * time.Second)
			subscriberCancel()

			opt := cmpopts.IgnoreFields(DataChangeRecord{}, "CommitTimestamp", "ServerTransactionID")
			if diff := cmp.Diff(consumer.records, test.expected, opt); diff != "" {
				t.Errorf("diff = %v", diff)
			}
		})
	}
}
