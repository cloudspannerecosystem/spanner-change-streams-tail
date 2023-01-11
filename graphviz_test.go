package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"github.com/google/go-cmp/cmp"
)

func TestPartitionVisualizer(t *testing.T) {
	for _, test := range []struct {
		desc        string
		readResults []*changestreams.ReadResult
		expected    string
	}{
		{
			desc:        "empty partition results",
			readResults: []*changestreams.ReadResult{},
			expected: `digraph {
  node [shape=record];
  "root" [label="{token|start_timestamp|record_sequence}|{{root}|{}|{}}"];
}
`,
		},
		{
			desc: "simple split/join results",
			readResults: []*changestreams.ReadResult{
				{
					PartitionToken: "",
					ChangeRecords: []*changestreams.ChangeRecord{
						{
							ChildPartitionsRecords: []*changestreams.ChildPartitionsRecord{
								{
									StartTimestamp: mustParseTime(t, "2022-12-04T18:00:00Z"),
									RecordSequence: "00000001",
									ChildPartitions: []*changestreams.ChildPartition{
										{
											Token:                 "a",
											ParentPartitionTokens: []string{},
										},
									},
								},
							},
						},
					},
				},
				{
					PartitionToken: "a",
					ChangeRecords: []*changestreams.ChangeRecord{
						{
							ChildPartitionsRecords: []*changestreams.ChildPartitionsRecord{
								{
									StartTimestamp: mustParseTime(t, "2022-12-04T19:00:00Z"),
									RecordSequence: "00000001",
									ChildPartitions: []*changestreams.ChildPartition{
										{
											Token:                 "b",
											ParentPartitionTokens: []string{"a"},
										},
									},
								},
								{
									StartTimestamp: mustParseTime(t, "2022-12-04T19:00:00Z"),
									RecordSequence: "00000002",
									ChildPartitions: []*changestreams.ChildPartition{
										{
											Token:                 "c",
											ParentPartitionTokens: []string{"a"},
										},
									},
								},
							},
						},
					},
				},
				{
					PartitionToken: "b",
					ChangeRecords: []*changestreams.ChangeRecord{
						{
							ChildPartitionsRecords: []*changestreams.ChildPartitionsRecord{
								{
									StartTimestamp: mustParseTime(t, "2022-12-04T20:00:00Z"),
									RecordSequence: "00000001",
									ChildPartitions: []*changestreams.ChildPartition{
										{
											Token:                 "d",
											ParentPartitionTokens: []string{"b", "c"},
										},
									},
								},
							},
						},
					},
				},
				{
					PartitionToken: "c",
					ChangeRecords: []*changestreams.ChangeRecord{
						{
							ChildPartitionsRecords: []*changestreams.ChildPartitionsRecord{
								{
									StartTimestamp: mustParseTime(t, "2022-12-04T20:00:00Z"),
									RecordSequence: "00000001",
									ChildPartitions: []*changestreams.ChildPartition{
										{
											Token:                 "d",
											ParentPartitionTokens: []string{"b", "c"},
										},
									},
								},
							},
						},
					},
				},
				{
					PartitionToken: "d",
					ChangeRecords:  []*changestreams.ChangeRecord{},
				},
			},
			expected: `digraph {
  node [shape=record];
  "a" [label="{token|start_timestamp|record_sequence}|{{a}|{2022-12-04T18:00:00Z}|{00000001}}"];
  "b" [label="{token|start_timestamp|record_sequence}|{{b}|{2022-12-04T19:00:00Z}|{00000001}}"];
  "c" [label="{token|start_timestamp|record_sequence}|{{c}|{2022-12-04T19:00:00Z}|{00000002}}"];
  "d" [label="{token|start_timestamp|record_sequence}|{{d}|{2022-12-04T20:00:00Z}|{00000001}}"];
  "root" [label="{token|start_timestamp|record_sequence}|{{root}|{}|{}}"];
  "root" -> "a"
  "a" -> "b"
  "a" -> "c"
  "b" -> "d"
  "c" -> "d"
}
`,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			var out bytes.Buffer
			visualizer := NewPartitionVisualizer(&out)
			for _, r := range test.readResults {
				visualizer.Read(r)
			}
			visualizer.Draw()

			if diff := cmp.Diff(out.String(), test.expected); diff != "" {
				t.Errorf("visualizer has diff = %v", diff)
			}
		})
	}
}

func mustParseTime(t *testing.T, s string) time.Time {
	parsed, err := time.ParseInLocation(time.RFC3339, s, time.UTC)
	if err != nil {
		t.Fatalf("failed to parse time: %v", err)
	}
	return parsed
}
