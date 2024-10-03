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
	"time"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// BatchedMessageIteratorFunc a utility type to allow functions to implement broker.BatchedMessageIterator.
type BatchedMessageIteratorFunc func(context.Context) ([]broker.Message, error)

func (f BatchedMessageIteratorFunc) NextBatch(ctx context.Context) ([]broker.Message, error) {
	return f(ctx)
}

// MessageIteratorIntoBatchedMessageIterator turns the given broker.MessageIterator into broker.BatchedMessageIterator.
func MessageIteratorIntoBatchedMessageIterator( //nolint:ireturn //have to return interface
	iterator broker.MessageIterator,
	settings IntoBatchedMessageIteratorSettings,
) broker.BatchedMessageIterator {
	return BatchedMessageIteratorFunc(func(ctx context.Context) ([]broker.Message, error) {
		batchCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
		defer cancel()

		messages := make([]broker.Message, 0, settings.BatchSize)
		for {
			message, err := iterator.NextMessage(batchCtx)
			if err != nil {
				if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && ctx.Err() == nil {
					break
				}

				return nil, err
			}
			messages = append(messages, message)
			if len(messages) == settings.BatchSize {
				break
			}
		}

		return messages, nil
	})
}

// IntoBatchedMessageIteratorSettings holds the settings to be used in MessageIteratorIntoBatchedMessageIterator.
type IntoBatchedMessageIteratorSettings struct {
	// BatchSize the upper bound of the size of the batch.
	BatchSize int

	// Timeout the maximum amount of time the stream will wait for BatchSize number of messages.
	Timeout time.Duration
}
