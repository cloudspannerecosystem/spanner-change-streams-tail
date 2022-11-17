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
	"fmt"
	"io"
	"sync"
	"time"
)

const rootPartitionToken = "root"

type Partition struct {
	Token          string
	StartTimestamp time.Time
	RecordSequence string
	Parents        []*Partition
}

func (p *Partition) ShortenToken() string {
	length := 20
	if len(p.Token) < 20 {
		length = len(p.Token)
	}
	return p.Token[0:length]
}

// Assert that PartitionVisualizer implements Consumer.
var _ Consumer = (*PartitionVisualizer)(nil)

type PartitionVisualizer struct {
	partitions map[string]*Partition
	mu         sync.Mutex
	out        io.Writer
}

func NewPartitionVisualizer(out io.Writer) *PartitionVisualizer {
	partitions := make(map[string]*Partition)
	// Root partition.
	partitions[rootPartitionToken] = &Partition{Token: rootPartitionToken}
	return &PartitionVisualizer{
		partitions: partitions,
		out:        out,
	}
}

func (v *PartitionVisualizer) Consume(partitionToken string, result *ReadResult) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	for _, changeRecord := range result.ChangeRecords {
		for _, partitionRecord := range changeRecord.ChildPartitionsRecords {
			for _, childPartition := range partitionRecord.ChildPartitions {
				token := childPartition.Token
				if _, ok := v.partitions[token]; ok {
					continue
				}

				var parents []*Partition
				for _, parentToken := range childPartition.ParentPartitionTokens {
					parent, ok := v.partitions[parentToken]
					// It's possible that parent partition is not included in the specified time range.
					if !ok {
						parent = &Partition{
							Token: parentToken,
						}
						v.partitions[parentToken] = parent
					}
					parents = append(parents, parent)
				}
				if len(parents) == 0 {
					parents = append(parents, v.partitions[rootPartitionToken])
				}

				childPartition := &Partition{
					Token:          token,
					StartTimestamp: partitionRecord.StartTimestamp,
					RecordSequence: partitionRecord.RecordSequence,
					Parents:        parents,
				}
				v.partitions[token] = childPartition
			}
		}
	}
	return nil
}

func (v *PartitionVisualizer) Draw() {
	fmt.Fprintf(v.out, "digraph {\n")
	fmt.Fprintf(v.out, "  node [shape=record];\n")
	for _, partition := range v.partitions {
		t := partition.ShortenToken()
		fmt.Fprintf(v.out, `  "%s" [label="{token|start_timestamp|record_sequence}|{{%s}|{%s}|{%s}}"];`, t, t, partition.StartTimestamp.Format(time.RFC3339), partition.RecordSequence)
		fmt.Fprintln(v.out, "")
	}
	for _, partition := range v.partitions {
		for _, parent := range partition.Parents {
			fmt.Fprintf(v.out, `  "%s" -> "%s"`, parent.ShortenToken(), partition.ShortenToken())
			fmt.Fprintln(v.out, "")
		}
	}
	fmt.Fprintf(v.out, "}\n")
}
