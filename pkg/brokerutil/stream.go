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

// MessageStreamFunc a utility type to allow functions to implement broker.MessageStream.
type MessageStreamFunc func(ctx context.Context) (<-chan broker.Message, <-chan error)

func (f MessageStreamFunc) Stream(ctx context.Context) (<-chan broker.Message, <-chan error) {
	return f(ctx)
}

// ReceiverIntoMessageStream turns a broker.Receiver into broker.MessageStream.
// The returned error channel will contain at most one error, if the broker.Receiver instance shut down due to an error.
//
// This means the returned error channel is safe to ignore until the stream is processed completely.
func ReceiverIntoMessageStream(receiver broker.Receiver) broker.MessageStream { //nolint:ireturn //have to return interface
	return MessageStreamFunc(func(ctx context.Context) (<-chan broker.Message, <-chan error) {
		return StreamifyReceiver(ctx, receiver)
	})
}

// MessageIteratorIntoMessageStream turns a broker.MessageIterator into broker.MessageStream.
// The returned error channel will contain at most one error: the first error returned by the broker.MessageIterator.
//
// This means the returned error channel is safe to ignore until the stream is processed completely.
// Both channels are closed when the first error is returned or when context is done.
func MessageIteratorIntoMessageStream(iterator broker.MessageIterator) broker.MessageStream { //nolint:ireturn //have to return interface
	return MessageStreamFunc(func(ctx context.Context) (<-chan broker.Message, <-chan error) {
		results := make(chan broker.Message, 1)
		errc := make(chan error)

		go func() {
			// the channels for batches and for errors will close when the context is canceled or when an error happens otherwise.
			defer close(results)
			defer close(errc)

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				message, err := iterator.NextMessage(ctx)
				// We need to check if the context got canceled in between the select and the poll.
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						errc <- err
					}
					// return and close both channels; errc will contain at most one error and doesn't need to be checked until results is closed.
					return
				}
				results <- message
			}
		}()

		return results, errc
	})
}

// MessageBatchIteratorIntoMessageStream turns a broker.BatchedMessageIterator into broker.MessageStream.
// The returned error channel will contain at most one error: the first error returned by the broker.MessageIterator.
//
// This means the returned error channel is safe to ignore until the stream is processed completely.
// Both channels are closed when the first error is returned or when context is done.
func MessageBatchIteratorIntoMessageStream(iterator broker.BatchedMessageIterator) broker.MessageStream { //nolint:ireturn //have to return interface
	return MessageStreamFunc(func(ctx context.Context) (<-chan broker.Message, <-chan error) {
		results := make(chan broker.Message, 1)
		errc := make(chan error)

		go func() {
			// the channels for batches and for errors will close when the context is canceled or when an error happens otherwise.
			defer close(results)
			defer close(errc)

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				batch, err := iterator.NextBatch(ctx)
				// We need to check if the context got canceled in between the select and the poll.
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						errc <- err
					}
					// return and close both channels; errc will contain at most one error and doesn't need to be checked until results is closed.
					return
				}
				for i := range batch {
					results <- batch[i]
				}
			}
		}()

		return results, errc
	})
}

// BatchedMessageStreamFunc a utility type to allow functions to implement broker.BatchedMessageStream.
type BatchedMessageStreamFunc func(ctx context.Context) (<-chan []broker.Message, <-chan error)

func (f BatchedMessageStreamFunc) BatchStream(ctx context.Context) (<-chan []broker.Message, <-chan error) {
	return f(ctx)
}

// MessageIteratorIntoBatchedMessageStream turns a broker.MessageIterator into broker.BatchedMessageStream.
// The returned error channel will contain at most one error: the first error returned by the broker.MessageIterator.
//
// This means the returned error channel is safe to ignore until the stream is processed completely.
func MessageIteratorIntoBatchedMessageStream( //nolint:ireturn //have to return interface
	iterator broker.MessageIterator,
	settings IntoBatchedMessageStreamSettings,
) broker.BatchedMessageStream {
	return BatchedMessageIteratorIntoBatchedMessageStream(MessageIteratorIntoBatchedMessageIterator(
		iterator,
		IntoBatchedMessageIteratorSettings{
			BatchSize: settings.BatchSize,
			Timeout:   settings.BatchTimeout,
		},
	))
}

// IntoBatchedMessageStreamSettings holds the settings to be used in MessageIteratorIntoBatchedMessageStream.
type IntoBatchedMessageStreamSettings struct {
	BatchSize    int
	BatchTimeout time.Duration
}

// BatchedMessageIteratorIntoBatchedMessageStream turns a broker.BatchedMessageIterator into broker.BatchedMessageStream.
// The returned error channel will contain at most one error: the first error returned by the broker.BatchedMessageIterator.
//
// This means the returned error channel is safe to ignore until the stream is processed completely.
// Both channels are closed when the first error is returned or when context is done.
func BatchedMessageIteratorIntoBatchedMessageStream( //nolint:ireturn //have to return interface
	iterator broker.BatchedMessageIterator,
) broker.BatchedMessageStream {
	return BatchedMessageStreamFunc(func(ctx context.Context) (<-chan []broker.Message, <-chan error) {
		results := make(chan []broker.Message, 1)
		errc := make(chan error, 1)

		go func() {
			// the channels for batches and for errors will close when the context is canceled or when an error happens otherwise.
			defer close(results)
			defer close(errc)

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				batch, err := iterator.NextBatch(ctx)
				// We need to check if the context got canceled in between the select and the poll.
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						errc <- err
					}
					// return and close both channels; errc will contain at most one error and doesn't need to be checked until results is closed.
					return
				}
				if len(batch) > 0 {
					results <- batch
				}
			}
		}()

		return results, errc
	})
}

// ReceiverIntoBatchedMessageStream turns the given broker.Receiver into broker.BatchedMessageStream using the given IntoBatchedMessageStreamSettings.
//
// This is achieved by first turning the receiver into a broker.MessageStream, and then windowing (batching) the stream.
func ReceiverIntoBatchedMessageStream( //nolint:ireturn //have to return interface
	receiver broker.Receiver,
	settings IntoBatchedMessageStreamSettings,
) broker.BatchedMessageStream {
	return MessageStreamIntoBatchedMessageStream(
		ReceiverIntoMessageStream(receiver),
		settings,
	)
}

// MessageStreamIntoBatchedMessageStream turns the given broker.MessageStream into broker.BatchedMessageStream,
// using the given IntoBatchedMessageStreamSettings.
//
// This is a simple implementation of windowing,
// which waits for the current batch to reach IntoBatchedMessageStreamSettings.BatchSize
// or for IntoBatchedMessageStreamSettings.BatchTimeout to elapse.
// The results channel is closed once the underlying broker.MessageStream channel gets closed.
func MessageStreamIntoBatchedMessageStream( //nolint:gocognit,ireturn //reducing cognitive complexity would have a negative effect on readability
	messageStream broker.MessageStream,
	settings IntoBatchedMessageStreamSettings,
) broker.BatchedMessageStream {
	return BatchedMessageStreamFunc(func(ctx context.Context) (<-chan []broker.Message, <-chan error) {
		stream, errc := messageStream.Stream(ctx)

		batches := make(chan []broker.Message, 1)

		go func() {
			defer close(batches)

			batch := make([]broker.Message, 0, settings.BatchSize)

			for {
				// We don't have to check if ctx is Done, since the closing of the stream implies it.
				select {
				case <-time.After(settings.BatchTimeout):
					if len(batch) > 0 {
						batches <- batch
						batch = make([]broker.Message, 0, settings.BatchSize)
					}
				case message, ok := <-stream:
					if !ok {
						if len(batch) > 0 {
							batches <- batch
						}

						return
					}
					batch = append(batch, message)
					if len(batch) == settings.BatchSize {
						batches <- batch
						batch = make([]broker.Message, 0, settings.BatchSize)
					}
				}
			}
		}()

		return batches, errc
	})
}
