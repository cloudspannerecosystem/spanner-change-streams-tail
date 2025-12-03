package changestreams

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instancesapi "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultEmulatorHost = "localhost"
	defaultEmulatorPort = "9010"

	projectID  = "project-id"
	instanceID = "instance-id"
	tableID    = "test_table"
	streamID   = "test_stream"
)

type SpannerEmulatorContainer struct {
	endpoint string
	dbname   string
	dbid     string
}

func RunSpannerForTesting(t testing.TB, containerHost, containerPrivatePort string, dialect databasepb.DatabaseDialect) *SpannerEmulatorContainer {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         "spanner-" + uuid.New().String(),
		Repository:   "gcr.io/cloud-spanner-emulator/emulator",
		Tag:          "1.5.45",
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", containerPrivatePort)},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	containerPublicPort := resource.GetPort(fmt.Sprintf("%s/tcp", containerPrivatePort))
	t.Setenv("SPANNER_EMULATOR_HOST", fmt.Sprintf("%s:%s", containerHost, containerPublicPort))

	t.Log("using spanner emulator", os.Getenv("SPANNER_EMULATOR_HOST"))

	instancesClient, err := instancesapi.NewInstanceAdminClient(t.Context())
	require.NoError(t, err)
	defer instancesClient.Close()

	createInstanceOp, err := instancesClient.CreateInstance(t.Context(), &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + projectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      "emulator-config",
			DisplayName: "Test Instance",
			NodeCount:   3,
		},
	})
	require.NoError(t, err)

	spannerInstance, err := createInstanceOp.Wait(t.Context())
	require.NoError(t, err)

	adminClient, err := database.NewDatabaseAdminClient(t.Context())
	require.NoError(t, err)
	defer adminClient.Close()

	dbID := "database-id"

	op, err := adminClient.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
		Parent:          spannerInstance.Name,
		CreateStatement: "CREATE DATABASE `" + dbID + "`",
		ExtraStatements: []string{
			fmt.Sprintf(`CREATE TABLE %s (
				id INT64 NOT NULL,
				name STRING(100),
				value INT64
			) PRIMARY KEY (id)`, tableID),
			fmt.Sprintf("CREATE CHANGE STREAM %s FOR %s", streamID, tableID),
		},
	})
	require.NoError(t, err)

	db, err := op.Wait(t.Context())
	require.NoError(t, err)

	t.Log("created database with change stream")

	endpoint := fmt.Sprintf("%s:%s", defaultEmulatorHost, containerPublicPort)

	return &SpannerEmulatorContainer{endpoint: endpoint, dbname: db.Name, dbid: dbID}
}

// TestReaderWithEmulator tests that the reader correctly reads three rows written in a single transaction
// and that all three changes share the same commit timestamp and server transaction ID.
func TestReaderWithEmulator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dialects := []databasepb.DatabaseDialect{
		databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
		//databasepb.DatabaseDialect_POSTGRESQL,
	}

	for _, dialect := range dialects {
		t.Run(dialect.String(), func(t *testing.T) {
			spannerEmulatorContainer := RunSpannerForTesting(t, defaultEmulatorHost, defaultEmulatorPort, dialect)

			opts := []option.ClientOption{
				option.WithEndpoint(spannerEmulatorContainer.endpoint),
				option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
				option.WithoutAuthentication(),
			}

			dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, spannerEmulatorContainer.dbid)
			client, err := spanner.NewClient(t.Context(), dbPath, opts...)
			require.NoError(t, err)
			defer client.Close()

			reader, err := NewReaderWithConfig(t.Context(), projectID, instanceID, spannerEmulatorContainer.dbid, streamID, Config{
				SpannerClientOptions: opts,
				HeartbeatInterval:    1 * time.Second,
			})
			require.NoError(t, err)
			defer reader.Close()

			t.Log("Created changestream reader")

			// Channel to collect data change records
			recordsChan := make(chan *DataChangeRecord, 3)

			// Start reader in parallel
			readerCtx, readerCancel := context.WithCancel(context.Background())
			defer readerCancel()

			var wg errgroup.Group
			wg.Go(func() error {
				return reader.Read(readerCtx, func(result *ReadResult) error {
					for _, changeRecord := range result.ChangeRecords {
						for _, dataChange := range changeRecord.DataChangeRecords {
							recordsChan <- dataChange
							t.Logf("Received data change record: table=%s, mod_type=%s, commit_ts=%v, txn_id=%s, is_last=%v, num_records=%d, transaction_tag=%s",
								dataChange.TableName, dataChange.ModType, dataChange.CommitTimestamp,
								dataChange.ServerTransactionID, dataChange.IsLastRecordInTransactionInPartition,
								dataChange.NumberOfRecordsInTransaction, dataChange.TransactionTag)
						}
					}
					return nil
				})
			})

			commitTimestamp, err := client.ReadWriteTransactionWithOptions(t.Context(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				mutations := []*spanner.Mutation{
					spanner.Insert(tableID, []string{"id", "name", "value"}, []interface{}{1, "row1", 100}),
					spanner.Insert(tableID, []string{"id", "name", "value"}, []interface{}{2, "row2", 200}),
					spanner.Insert(tableID, []string{"id", "name", "value"}, []interface{}{3, "row3", 300}),
				}
				return txn.BufferWrite(mutations)
			}, spanner.TransactionOptions{TransactionTag: uuid.New().String()})
			require.NoError(t, err)
			t.Logf("Wrote 3 rows in transaction with commit timestamp: %v", commitTimestamp)

			commitTimestamp, err = client.ReadWriteTransactionWithOptions(t.Context(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				mutations := []*spanner.Mutation{
					spanner.Update(tableID, []string{"id", "value"}, []interface{}{1, 1_000}),
					spanner.Update(tableID, []string{"id", "value"}, []interface{}{2, 1_000}),
					spanner.Delete(tableID, spanner.Key{"3"}),
				}
				return txn.BufferWrite(mutations)
			}, spanner.TransactionOptions{TransactionTag: uuid.New().String()})
			require.NoError(t, err)
			t.Logf("Updated 3 rows in transaction with commit timestamp: %v", commitTimestamp)

			var records []*DataChangeRecord
			collectTimeout := time.After(15 * time.Second)
			collecting := true

			for collecting {
				select {
				case record := <-recordsChan:
					records = append(records, record)
				case <-collectTimeout:
					collecting = false
				}
			}

			readerCancel()
			err = wg.Wait()
			require.ErrorContains(t, err, "context canceled")
			close(recordsChan)

			// one record for the 3 inserts, one for the updates, one for the deletes
			require.Len(t, records, 3)

			type assertStruct struct {
				modType   string
				modNumber int
			}

			var asserts = map[*DataChangeRecord]assertStruct{
				records[0]: {modType: "INSERT", modNumber: 3},
				records[1]: {modType: "UPDATE", modNumber: 2},
				records[2]: {modType: "DELETE", modNumber: 1},
			}

			for _, record := range records {
				require.Equal(t, asserts[record].modNumber, len(record.Mods))
				require.Equal(t, asserts[record].modType, record.ModType)

				require.NotEmpty(t, record.TransactionTag)
			}
		})
	}
}
