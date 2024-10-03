// Copyright 2024 Syntio Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package brokerutil

import (
	"context"
	"sync"

	"github.com/dataphos/lib-batchproc/pkg/batchproc"
	"github.com/dataphos/lib-brokers/pkg/broker"
)

func GroupBatchBy(records []broker.Record, grouper func(record broker.Record) string) map[string][]broker.Record {
	groups := make(map[string][]broker.Record)
	for _, record := range records {
		groups[grouper(record)] = append(groups[grouper(record)], record)
	}

	return groups
}

func GroupBatchByKeyContents(records []broker.Record) map[string][]broker.Record {
	return GroupBatchBy(records, func(record broker.Record) string {
		return record.Key
	})
}

// AckAll acks all given broker.Message instances.
//
// For performance reasons, each message is acked in its own goroutine.
func AckAll(messages ...broker.Message) {
	batchSize := len(messages)
	if batchSize == 0 {
		return
	}

	_ = batchproc.Process(context.Background(), batchSize, batchSize, func(_ context.Context, i int, _ int) error {
		messages[i].Ack()

		return nil
	})
}

// NackAll acks all given broker.Message instances.
//
// For performance reasons, each message is nacked in its own goroutine.
func NackAll(messages ...broker.Message) {
	batchSize := len(messages)
	if batchSize == 0 {
		return
	}

	_ = batchproc.Process(context.Background(), batchSize, batchSize, func(_ context.Context, i int, _ int) error {
		messages[i].Nack()

		return nil
	})
}

// CommitHighestOffsets finds and commits only the highest offset for each partition in a batch. It is redundant
// to commit every message in a batch explicitly since committing only the last message achieves the same effect.
//
// For performance reasons, the last offset for each partition is committed in its own goroutine.
func CommitHighestOffsets(records []broker.Record) {
	var waitGroup sync.WaitGroup

	maxOffsetRecords := make(map[int64]broker.Record)

	for _, record := range records {
		currMax := maxOffsetRecords[record.Partition].Offset

		if record.Offset >= currMax {
			maxOffsetRecords[record.Partition] = record
		}
	}

	waitGroup.Add(len(maxOffsetRecords))

	for _, record := range maxOffsetRecords {
		record := record

		go func() {
			defer waitGroup.Done()
			record.CommitFunc()
		}()
	}

	waitGroup.Wait()
}
