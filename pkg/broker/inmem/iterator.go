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

package inmem

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// MessageSpawnIterator returns messages with incrementing IDs starting 0 and going to AmountToReturn-1.
// The messages are also stored in an internal slice.
type MessageSpawnIterator struct {
	Messages       []broker.Message
	AmountToReturn int
	Timeout        time.Duration
	ErrorToReturn  error
}

// NextMessage returns a message and can block for Timeout to simulate some delay.
// When the IDs are exhausted, it returns a predefined error (for example, context.Canceled).
func (it *MessageSpawnIterator) NextMessage(ctx context.Context) (broker.Message, error) {
	select {
	case <-ctx.Done():
		return broker.Message{}, context.Canceled
	case <-time.After(it.Timeout):
		remaining := it.AmountToReturn - len(it.Messages)
		if remaining <= 0 {
			if it.ErrorToReturn != nil {
				return broker.Message{}, it.ErrorToReturn
			}

			<-ctx.Done()

			return broker.Message{}, context.Canceled
		}

		id := fmt.Sprintf("%d", len(it.Messages))
		message := broker.Message{ID: id}
		it.Messages = append(it.Messages, message)

		return message, nil
	}
}

// BatchSpawnIterator is a batched variant of MessageSpawnIterator.
type BatchSpawnIterator struct {
	Messages       []broker.Message
	AmountToReturn int
	BatchSize      int
	Timeout        time.Duration
	ErrorToReturn  error
}

func (it *BatchSpawnIterator) NextBatch(ctx context.Context) ([]broker.Message, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	case <-time.After(it.Timeout):
		remaining := it.AmountToReturn - len(it.Messages)
		if remaining <= 0 {
			return nil, it.ErrorToReturn
		}

		if remaining > it.BatchSize {
			remaining = it.BatchSize
		}

		batch := make([]broker.Message, remaining)
		for messageIndex := range batch {
			id := strconv.Itoa(messageIndex + len(it.Messages))
			batch[messageIndex] = broker.Message{ID: id}
		}

		it.Messages = append(it.Messages, batch...)

		return batch, nil
	}
}
