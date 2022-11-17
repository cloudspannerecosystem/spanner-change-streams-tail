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
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

const (
	formatText = "text"
	formatJSON = "json"
)

type VerboseLog struct {
	PartitionToken string          `json:"partition_token"`
	ChangeRecords  []*ChangeRecord `json:"change_record"`
}

// Assert that Logger implements Consumer.
var _ Consumer = (*Logger)(nil)

type Logger struct {
	out     io.Writer
	format  string
	verbose bool
	mu      sync.Mutex
}

func (l *Logger) Consume(partitionToken string, result *ReadResult) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.verbose {
		log := VerboseLog{
			PartitionToken: partitionToken,
			ChangeRecords:  result.ChangeRecords,
		}
		return json.NewEncoder(l.out).Encode(&log)
	}

	// Only prints the data change records.
	for _, changeRecord := range result.ChangeRecords {
		for _, r := range changeRecord.DataChangeRecords {
			switch l.format {
			case formatJSON:
				if err := json.NewEncoder(l.out).Encode(r); err != nil {
					return err
				}
			case formatText:
				modsJSON, err := json.Marshal(r.Mods)
				if err != nil {
					return err
				}
				fmt.Fprintf(l.out, "%s | %s | %s | %s\n", r.CommitTimestamp, r.ModType, r.TableName, modsJSON)
			default:
				return fmt.Errorf("invalid format: %s", l.format)
			}
		}
	}

	return nil
}
