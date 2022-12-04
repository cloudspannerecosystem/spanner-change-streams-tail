package main

import (
	"bytes"
	"testing"
	"time"
)

type partitionResult struct {
	partitionToken string
	result         *ReadResult
}

func TestPartitionVisualizer(t *testing.T) {
	for _, test := range []struct {
		desc             string
		partitionResults []*partitionResult
		expected         string
	}{
		{
			desc:             "empty partition results",
			partitionResults: []*partitionResult{},
			expected: `digraph {
  node [shape=record];
  "root" [label="{token|start_timestamp|record_sequence}|{{root}|{}|{}}"];
}
`,
		},
		{
			desc: "simple split/join results",
			partitionResults: []*partitionResult{
				{
					partitionToken: "",
					result: &ReadResult{
						ChangeRecords: []*ChangeRecord{
							{
								ChildPartitionsRecords: []*ChildPartitionsRecord{
									{
										StartTimestamp: mustParseTime(t, "2022-12-04T18:00:00+09:00"),
										RecordSequence: "00000001",
										ChildPartitions: []*ChildPartition{
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
				},
				{
					partitionToken: "a",
					result: &ReadResult{
						ChangeRecords: []*ChangeRecord{
							{
								ChildPartitionsRecords: []*ChildPartitionsRecord{
									{
										StartTimestamp: mustParseTime(t, "2022-12-04T19:00:00+09:00"),
										RecordSequence: "00000001",
										ChildPartitions: []*ChildPartition{
											{
												Token:                 "b",
												ParentPartitionTokens: []string{"a"},
											},
										},
									},
									{
										StartTimestamp: mustParseTime(t, "2022-12-04T19:00:00+09:00"),
										RecordSequence: "00000002",
										ChildPartitions: []*ChildPartition{
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
				},
				{
					partitionToken: "b",
					result: &ReadResult{
						ChangeRecords: []*ChangeRecord{
							{
								ChildPartitionsRecords: []*ChildPartitionsRecord{
									{
										StartTimestamp: mustParseTime(t, "2022-12-04T20:00:00+09:00"),
										RecordSequence: "00000001",
										ChildPartitions: []*ChildPartition{
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
				},
				{
					partitionToken: "c",
					result: &ReadResult{
						ChangeRecords: []*ChangeRecord{
							{
								ChildPartitionsRecords: []*ChildPartitionsRecord{
									{
										StartTimestamp: mustParseTime(t, "2022-12-04T20:00:00+09:00"),
										RecordSequence: "00000001",
										ChildPartitions: []*ChildPartition{
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
				},
				{
					partitionToken: "d",
					result: &ReadResult{
						ChangeRecords: []*ChangeRecord{},
					},
				},
			},
			expected: `digraph {
  node [shape=record];
  "a" [label="{token|start_timestamp|record_sequence}|{{a}|{2022-12-04T18:00:00+09:00}|{00000001}}"];
  "b" [label="{token|start_timestamp|record_sequence}|{{b}|{2022-12-04T19:00:00+09:00}|{00000001}}"];
  "c" [label="{token|start_timestamp|record_sequence}|{{c}|{2022-12-04T19:00:00+09:00}|{00000002}}"];
  "d" [label="{token|start_timestamp|record_sequence}|{{d}|{2022-12-04T20:00:00+09:00}|{00000001}}"];
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
			for _, r := range test.partitionResults {
				visualizer.Consume(r.partitionToken, r.result)
			}
			visualizer.Draw()

			if out.String() != test.expected {
				t.Errorf("visualizer got = %q, but want = %q", out.String(), test.expected)
			}
		})
	}
}

func mustParseTime(t *testing.T, s string) time.Time {
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("failed to parse time: %v", err)
	}
	return parsed
}
