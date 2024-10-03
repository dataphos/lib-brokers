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

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// StreamifyReceiver calls the receive method of the given broker.Receiver with a handler function
// that places each received message onto the returned channel
//
// The returned channel is closed once the context is canceled (or deadline exceeded).
//
// Because Receive might return an error, an error channel is also returned, which will contain at most a single error.
func StreamifyReceiver(ctx context.Context, receiver broker.Receiver) (<-chan broker.Message, <-chan error) {
	results := make(chan broker.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errc)

		if err := receiver.Receive(ctx, func(ctx context.Context, message broker.Message) {
			results <- message
		}); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			errc <- err
		}
	}()

	return results, errc
}

type MessageIterationResult struct {
	Message broker.Message
	Err     error
}

// StreamifyMessageIterator polls the broker.MessageIterator for new messages and return a channel (stream) of MessageIterationResult.
//
// The added benefit of this is that the next poll is made as soon as the last result is put on the channel,
// meaning that, if record processing is slower than the poll call, the consumer of the returned channel will
// never have to wait for poll to fetch the next record, potentially leading to a significant performance boost.
// This is also known as multiple (double) buffering.
//
// The returned channel is closed once the context is canceled (or deadline exceeded).
func StreamifyMessageIterator(ctx context.Context, iterator broker.MessageIterator) <-chan MessageIterationResult {
	results := make(chan MessageIterationResult, 10)

	go func() {
		defer close(results)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			message, err := iterator.NextMessage(ctx)
			// We need to check if the context got canceled in between the select and the poll.
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return
			}
			results <- MessageIterationResult{
				Message: message,
				Err:     err,
			}
		}
	}()

	return results
}

type MessageBatchIterationResult struct {
	Messages []broker.Message
	Err      error
}

// StreamifyMessageBatchIterator polls the broker.BatchedMessageIterator for batches of messages
// and return a channel (stream) of MessageIterationResult.
//
// The added benefit of this is that the next poll is made as soon as the last result is put on the channel,
// meaning that, if record processing is slower than the poll call, the consumer of the returned channel will
// never have to wait for poll to fetch the next record, potentially leading to a significant performance boost.
// This is also known as multiple (double) buffering.
//
// The returned channel is closed once the context is canceled (or deadline exceeded).
func StreamifyMessageBatchIterator(ctx context.Context, iterator broker.BatchedMessageIterator) <-chan MessageBatchIterationResult {
	results := make(chan MessageBatchIterationResult, 2)

	go func() {
		defer close(results)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			messages, err := iterator.NextBatch(ctx)
			// We need to check if the context got canceled in between the select and the poll.
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return
			}
			results <- MessageBatchIterationResult{
				Messages: messages,
				Err:      err,
			}
		}
	}()

	return results
}

type IterationResult struct {
	Record broker.Record
	Err    error
}

// StreamifyIterator polls the broker.RecordIterator for new records and return a channel (stream) of IterationResult.
//
// The added benefit of this is that the next poll is made as soon as the last result is put on the channel,
// meaning that, if record processing is slower than the poll call, the consumer of the returned channel will
// never have to wait for poll to fetch the next record, potentially leading to a significant performance boost.
// This is also known as multiple (double) buffering.
//
// The returned channel is closed once the context is canceled (or deadline exceeded).
func StreamifyIterator(ctx context.Context, iterator broker.RecordIterator) <-chan IterationResult {
	results := make(chan IterationResult, 1)

	go func() {
		defer close(results)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			record, err := iterator.NextRecord(ctx)
			// We need to check if the context got canceled in between the select and the poll.
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return
			}
			results <- IterationResult{
				Record: record,
				Err:    err,
			}
		}
	}()

	return results
}

type BatchIterationResult struct {
	Batch []broker.Record
	Err   error
}

// StreamifyBatchIterator polls the broker.BatchedIterator for batches and return a channel (stream) of BatchIterationResult.
//
// The added benefit of this is that the next poll is made as soon as the last result is put on the channel,
// meaning that, if record processing is slower than the poll call, the consumer of the returned channel will
// never have to wait for poll to fetch the next batch, potentially leading to a significant performance boost.
// This is also known as multiple (double) buffering.
//
// The returned channel is closed once the context is canceled (or deadline exceeded).
func StreamifyBatchIterator(ctx context.Context, iterator broker.BatchedIterator) <-chan BatchIterationResult {
	results := make(chan BatchIterationResult, 1)

	go func() {
		defer close(results)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			batch, err := iterator.NextBatch(ctx)
			// We need to check if the context got canceled in between the select and the poll.
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return
			}

			if len(batch) > 0 || err != nil {
				results <- BatchIterationResult{
					Batch: batch,
					Err:   err,
				}
			}
		}
	}()

	return results
}
