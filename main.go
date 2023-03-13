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

// spanner-change-streams-tail is a tool to tail Cloud Spanner Change Streams.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
)

func usage() {
	command := os.Args[0]
	fmt.Printf(`Usage:
  %s [OPTIONS]

Options:
  -p, --project=  (required)   GCP Project ID
  -i, --instance= (required)   Cloud Spanner Instance ID
  -d, --database= (required)   Cloud Spanner Database ID
  -s, --stream=   (required)   Cloud Spanner Change Stream ID
  -f, --format=                Output format [text|json] (default: text)
      --start=                 Start timestamp with RFC3339 format (default: current timestamp)
      --end=                   End timestamp with RFC3339 format (default: none)
      --role=                  Database role for fine-grained access control
      --visualize-partitions   Visualize the change stream partitions in Graphviz DOT

Help Options:
  -h, -help                    Show this help message
`, command)
}

func main() {
	var (
		projectID, instanceID, databaseID, streamID, format, start, end, role string
		startTimestamp, endTimestamp                                          time.Time
		verbose, visualizePartitions                                          bool
	)

	// Long options.
	flag.StringVar(&projectID, "project", "", "")
	flag.StringVar(&instanceID, "instance", "", "")
	flag.StringVar(&databaseID, "database", "", "")
	flag.StringVar(&streamID, "stream", "", "")
	flag.StringVar(&format, "format", formatText, "")
	flag.StringVar(&start, "start", "", "")
	flag.StringVar(&end, "end", "", "")
	flag.StringVar(&role, "role", "", "")
	flag.BoolVar(&verbose, "verbose", false, "")
	flag.BoolVar(&visualizePartitions, "visualize-partitions", false, "")

	// Short options.
	flag.StringVar(&projectID, "p", "", "")
	flag.StringVar(&instanceID, "i", "", "")
	flag.StringVar(&databaseID, "d", "", "")
	flag.StringVar(&streamID, "s", "", "")
	flag.StringVar(&format, "f", formatText, "")
	flag.BoolVar(&verbose, "v", false, "")

	flag.Usage = usage
	flag.Parse()

	// Validate required options.
	if projectID == "" || instanceID == "" || databaseID == "" || streamID == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Validate optional options.
	if format != formatText && format != formatJSON {
		exitf("invalid format: %s", format)
	}
	if start != "" {
		ts, err := time.Parse(time.RFC3339, start)
		if err != nil {
			exitf("invalid start timestamp: %v", err)
		}
		startTimestamp = ts
	}
	if end != "" {
		ts, err := time.Parse(time.RFC3339, end)
		if err != nil {
			exitf("invalid end timestamp: %v", err)
		}
		endTimestamp = ts
	}
	if visualizePartitions {
		if start == "" || end == "" {
			exitf("To visualize partitions, specify --start and --end options as well")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go handleInterrupt(cancel)

	config := changestreams.Config{
		StartTimestamp: startTimestamp,
		EndTimestamp:   endTimestamp,
		SpannerClientConfig: spanner.ClientConfig{
			SessionPoolConfig: spanner.DefaultSessionPoolConfig,
			DatabaseRole:      role,
		},
	}
	reader, err := changestreams.NewReaderWithConfig(ctx, projectID, instanceID, databaseID, streamID, config)
	if err != nil {
		exitf("failed to create a reader: %v", err)
	}
	defer reader.Close()

	if visualizePartitions {
		fmt.Fprintf(os.Stderr, "Reading the stream and analyzing partitions...\n\n")
		visualizer := NewPartitionVisualizer(os.Stdout)
		if err := reader.Read(ctx, visualizer.Read); err != nil {
			exitf("failed to read stream: %v", err)
		}
		visualizer.Draw()
		return
	}

	fmt.Fprintf(os.Stderr, "Reading the stream...\n")

	logger := &Logger{
		out:     os.Stdout,
		format:  format,
		verbose: verbose,
	}
	if err := reader.Read(ctx, logger.Read); err != nil {
		exitf("failed to read stream: %v", err)
	}
}

func exitf(format string, a ...interface{}) {
	message := fmt.Sprintf(format, a...)
	if !strings.HasSuffix(message, "\n") {
		message += "\n"
	}
	fmt.Fprint(os.Stderr, message)
	os.Exit(1)
}

func handleInterrupt(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	cancel()
}
