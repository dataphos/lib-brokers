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

	"golang.org/x/sync/errgroup"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// ReceiverFunc a utility type to allow functions to implement broker.Receiver.
type ReceiverFunc func(context.Context, func(context.Context, broker.Message)) error

func (f ReceiverFunc) Receive(ctx context.Context, h func(context.Context, broker.Message)) error {
	return f(ctx, h)
}

// FanInReceivers returns a fanned-in version of the given broker.Receiver instances.
// The returned broker.Receiver will cancel all receives once the first one returns with an error, or the context
// is canceled, whichever comes first.
func FanInReceivers(receivers ...broker.Receiver) broker.Receiver { //nolint:ireturn //have to return interface
	return ReceiverFunc(func(ctx context.Context, f func(context.Context, broker.Message)) error {
		errorGroup, ctx := errgroup.WithContext(ctx)
		for _, receiver := range receivers {
			receiver := receiver
			errorGroup.Go(func() error {
				return receiver.Receive(ctx, f)
			})
		}

		return errorGroup.Wait()
	})
}

// BatchedMessageIteratorIntoReceiver turns a broker.BatchedMessageIterator into a broker.Receiver.
func BatchedMessageIteratorIntoReceiver( //nolint:ireturn //have to return interface
	iterator broker.BatchedMessageIterator,
	settings IntoReceiverSettings,
) broker.Receiver {
	return &IteratorIntoReceiver{
		Iterator: iterator,
		Settings: settings,
	}
}

type IteratorIntoReceiver struct {
	Iterator broker.BatchedMessageIterator
	Settings IntoReceiverSettings
}

// IntoReceiverSettings holds the settings to be used in BatchedMessageIteratorIntoReceiver.
type IntoReceiverSettings struct {
	// NumGoroutines the number of concurrent workers that execute the given handle function.
	NumGoroutines int
}

func (r *IteratorIntoReceiver) Receive(ctx context.Context, function func(ctx context.Context, message broker.Message)) error {
	messages := make(chan broker.Message, 1)

	var waitGroup sync.WaitGroup

	waitGroup.Add(r.Settings.NumGoroutines)

	for i := 0; i < r.Settings.NumGoroutines; i++ {
		go func() {
			defer waitGroup.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case message, ok := <-messages:
					if !ok {
						return
					}

					function(ctx, message)
				}
			}
		}()
	}

	stream, errc := BatchedMessageIteratorIntoBatchedMessageStream(r.Iterator).BatchStream(ctx)

	for batch := range stream {
		drainBatch(messages, batch)
	}
	// The stream has ended because the parent context is canceled, so we wait for workers to realize this.
	waitGroup.Wait()

	// The messages channel needs to be closed so that we can drain it of any remaining messages (and nack them).
	close(messages)
	nackRemainingInMessageStream(messages)

	err := <-errc

	return err
}

func nackRemainingInMessageStream(messages <-chan broker.Message) {
	var waitGroup sync.WaitGroup

	for message := range messages {
		message := message

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			message.Nack()
		}()
	}

	waitGroup.Wait()
}

func drainBatch(messages chan<- broker.Message, batch []broker.Message) {
	for _, message := range batch {
		messages <- message
	}
}

// BatchedReceiverFunc a utility type to allow functions to implement broker.BatchedReceiver.
type BatchedReceiverFunc func(context.Context, func(context.Context, []broker.Message)) error

func (f BatchedReceiverFunc) ReceiveBatch(ctx context.Context, h func(context.Context, []broker.Message)) error {
	return f(ctx, h)
}

// MessageIteratorIntoBatchedReceiver turns a broker.MessageIterator into a broker.BatchedReceiver.
func MessageIteratorIntoBatchedReceiver( //nolint:ireturn //have to return interface
	iterator broker.MessageIterator,
	batchSettings IntoBatchedMessageIteratorSettings,
	settings IntoBatchedReceiverSettings,
) broker.BatchedReceiver {
	return BatchedMessageIteratorIntoBatchedReceiver(
		MessageIteratorIntoBatchedMessageIterator(iterator, batchSettings),
		settings,
	)
}

// BatchedMessageIteratorIntoBatchedReceiver turns a broker.BatchedMessageIterator into a broker.BatchedReceiver.
func BatchedMessageIteratorIntoBatchedReceiver( //nolint:ireturn //have to return interface
	iterator broker.BatchedMessageIterator,
	settings IntoBatchedReceiverSettings,
) broker.BatchedReceiver {
	return BatchedMessageStreamIntoBatchedReceiver(
		BatchedMessageIteratorIntoBatchedMessageStream(iterator),
		settings,
	)
}

// IntoBatchedReceiverSettings holds the settings to be used in BatchedMessageIteratorIntoReceiver.
type IntoBatchedReceiverSettings struct {
	// NumGoroutines the number of concurrent workers that execute the given handle function.
	NumGoroutines int
}

// BatchedMessageStreamIntoBatchedReceiver turns broker.BatchedMessageStream into broker.BatchedReceiver.
func BatchedMessageStreamIntoBatchedReceiver( //nolint:ireturn //have to return interface
	batchedMessageStream broker.BatchedMessageStream,
	settings IntoBatchedReceiverSettings,
) broker.BatchedReceiver {
	return BatchedReceiverFunc(func(ctx context.Context, function func(context.Context, []broker.Message)) error {
		stream, errc := batchedMessageStream.BatchStream(ctx)

		var waitGroup sync.WaitGroup
		waitGroup.Add(settings.NumGoroutines)
		for i := 0; i < settings.NumGoroutines; i++ {
			go func() {
				defer waitGroup.Done()

				for {
					batch, ok := <-stream
					if !ok {
						return
					}
					if len(batch) > 0 {
						function(ctx, batch)
					}
				}
			}()
		}

		waitGroup.Wait()
		nackRemainingInBatchStream(ctx, stream)

		err := <-errc

		return err
	})
}

func nackRemainingInBatchStream(_ context.Context, stream <-chan []broker.Message) {
	var waitGroup sync.WaitGroup

	for batch := range stream {
		if len(batch) == 0 {
			continue
		}

		batch := batch

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			NackAll(batch...)
		}()
	}

	waitGroup.Wait()
}

// ReceiverIntoBatchedReceiver turns broker.Receiver into broker.BatchedReceiver,
// by first turning broker.Receiver into broker.BatchedMessageStream.
func ReceiverIntoBatchedReceiver( //nolint:ireturn //have to return interface
	receiver broker.Receiver,
	batchSettings IntoBatchedMessageStreamSettings,
	receiverSettings IntoBatchedReceiverSettings,
) broker.BatchedReceiver {
	return BatchedMessageStreamIntoBatchedReceiver(
		ReceiverIntoBatchedMessageStream(receiver, batchSettings),
		receiverSettings,
	)
}
