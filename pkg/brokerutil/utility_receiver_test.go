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

package brokerutil_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/inmem"
	"github.com/dataphos/lib-brokers/pkg/brokerutil"
)

// FinishOrAbandon starts given function and waits for context to end.
// It returns if the function takes too long to return after context is done (gracePeriod)
// This allows test to finish early even if a mistake causes the function to hang indefinitely.
func FinishOrAbandon(ctx context.Context, worker func(), gracePeriod time.Duration) error {
	channel := make(chan int, 1)

	go func() {
		worker()
		channel <- 1
	}()

	<-ctx.Done()

	select {
	case <-time.After(gracePeriod):
		return errors.New("goroutine stuck for too long")
	case <-channel:
		return nil
	}
}

// CompareMessages compares messages at source with received messages.
func CompareMessages(messages []broker.Message, counts map[string]int) error {
	var missing []string

	for _, m := range messages {
		count := counts[m.ID]
		if count == 0 {
			missing = append(missing, m.ID)
		}
	}

	if len(missing) > 0 {
		errMessagesMissing := errors.New("messages missing")

		return fmt.Errorf("%w: %d messages missing out of %d. Missing IDs: %v",
			errMessagesMissing, len(missing), len(messages), missing)
	}

	return nil
}

func CountMessages(mut *sync.Mutex, idCounts map[string]int, messages ...broker.Message) {
	mut.Lock()
	for _, message := range messages {
		count := idCounts[message.ID]
		// increment count - if ID was not present, now it is set to 1.
		idCounts[message.ID] = count + 1
	}
	mut.Unlock()
}

// VerifyResults compares messages and checks for unexpected errors.
func VerifyResults(t *testing.T, runError error, pullErr error, expectedPullErr error, expectedMessages []broker.Message, consumedIDs map[string]int) { //nolint:lll //clashing with gofumpt
	t.Helper()

	if runError != nil {
		t.Error(runError)
	}

	if !errors.Is(pullErr, expectedPullErr) {
		if expectedPullErr != nil {
			t.Errorf("expected error (%v) but got (%v)", expectedPullErr, pullErr)
		} else {
			t.Errorf("expected no error from consumer but got: %v", pullErr)
		}
	}

	if len(expectedMessages) != len(consumedIDs) {
		t.Errorf("expected %d messages but got %d IDs", len(expectedMessages), len(consumedIDs))
	}

	dataErr := CompareMessages(expectedMessages, consumedIDs)
	if dataErr != nil {
		t.Error(dataErr)
	}
}

// simulates a batch iterator and converts it to a batch receiver via a stream (channel).
// Checks if the messages returned by the iterator are the received ones.
func TestBatchIteratorToBatchMessageStream(t *testing.T) {
	t.Parallel()

	iterator := &inmem.BatchSpawnIterator{
		Messages:       nil,
		AmountToReturn: 100,
		BatchSize:      10,
		Timeout:        time.Second / 10,
		ErrorToReturn:  nil,
	}
	stream := brokerutil.BatchedMessageIteratorIntoBatchedMessageStream(iterator)
	rec := brokerutil.BatchedMessageStreamIntoBatchedReceiver(stream, brokerutil.IntoBatchedReceiverSettings{NumGoroutines: 1})

	// The receiver runs until for enough time for the iterator to return all the batches and some more.
	pullDuration := iterator.Timeout*time.Duration(iterator.AmountToReturn/iterator.BatchSize+1) + time.Second*2

	ctx, cancel := context.WithTimeout(context.Background(), pullDuration)
	defer cancel()

	// time to allow the receiver to run before interrupting the test in case iterator hangs due to some bug.
	gracePeriod := time.Second * 10

	ids := map[string]int{}
	mut := sync.Mutex{}

	var pullErr error

	// do the pull.
	testErr := FinishOrAbandon(ctx, func() {
		pullErr = rec.ReceiveBatch(ctx, func(_ context.Context, batch []broker.Message) {
			CountMessages(&mut, ids, batch...)
		})
	}, gracePeriod)

	// compare messages.
	VerifyResults(t, testErr, pullErr, iterator.ErrorToReturn, iterator.Messages, ids)
}

// simulates an iterator that doesn't return any messages.
func TestBatchIteratorToBatchMessageStream_Empty(t *testing.T) {
	t.Parallel()

	iterator := &inmem.BatchSpawnIterator{
		Messages:       nil,
		AmountToReturn: 0,
		BatchSize:      5,
		Timeout:        time.Second / 2,
		ErrorToReturn:  nil,
	}
	stream := brokerutil.BatchedMessageIteratorIntoBatchedMessageStream(iterator)
	rec := brokerutil.BatchedMessageStreamIntoBatchedReceiver(stream, brokerutil.IntoBatchedReceiverSettings{NumGoroutines: 1})

	pullDuration := iterator.Timeout*time.Duration(iterator.AmountToReturn/iterator.BatchSize+1) + time.Second*2

	ctx, cancel := context.WithTimeout(context.Background(), pullDuration)
	defer cancel()

	gracePeriod := time.Second * 10

	ids := map[string]int{}
	mut := sync.Mutex{}

	var pullErr error

	testErr := FinishOrAbandon(ctx, func() {
		pullErr = rec.ReceiveBatch(ctx, func(_ context.Context, batch []broker.Message) {
			CountMessages(&mut, ids, batch...)
		})
	}, gracePeriod)
	VerifyResults(t, testErr, pullErr, iterator.ErrorToReturn, iterator.Messages, ids)
}

// checks if the Receive function returns the error that the iterator produced.
func TestBatchIteratorToBatchMessageStream_Error(t *testing.T) {
	t.Parallel()

	iterator := &inmem.BatchSpawnIterator{
		Messages:       nil,
		AmountToReturn: 100,
		BatchSize:      5,
		Timeout:        time.Second / 10,
		ErrorToReturn:  errors.New("simulated error"),
	}
	stream := brokerutil.BatchedMessageIteratorIntoBatchedMessageStream(iterator)
	rec := brokerutil.BatchedMessageStreamIntoBatchedReceiver(stream, brokerutil.IntoBatchedReceiverSettings{NumGoroutines: 1})

	pullDuration := iterator.Timeout*time.Duration(iterator.AmountToReturn/iterator.BatchSize+1) + time.Second*2

	ctx, cancel := context.WithTimeout(context.Background(), pullDuration)
	defer cancel()

	gracePeriod := time.Second * 10

	ids := map[string]int{}
	mut := sync.Mutex{}

	var pullErr error

	testErr := FinishOrAbandon(ctx, func() {
		pullErr = rec.ReceiveBatch(ctx, func(_ context.Context, batch []broker.Message) {
			CountMessages(&mut, ids, batch...)
		})
	}, gracePeriod)
	VerifyResults(t, testErr, pullErr, iterator.ErrorToReturn, iterator.Messages, ids)
}

// converts a message iterator to a received without batching and checks that all the produced messages are received.
func TestMessageIteratorToMessageReceiver(t *testing.T) {
	t.Parallel()

	iterator := &inmem.MessageSpawnIterator{
		Messages:       nil,
		AmountToReturn: 100,
		Timeout:        time.Second / 10,
		ErrorToReturn:  nil,
	}

	batched := brokerutil.MessageIteratorIntoBatchedMessageIterator(iterator,
		brokerutil.IntoBatchedMessageIteratorSettings{BatchSize: 1, Timeout: time.Second},
	)
	rec := brokerutil.IteratorIntoReceiver{
		Iterator: batched,
		Settings: brokerutil.IntoReceiverSettings{NumGoroutines: 1},
	}

	pullDuration := iterator.Timeout*time.Duration(iterator.AmountToReturn+1) + time.Second*5

	ctx, cancel := context.WithTimeout(context.Background(), pullDuration)
	defer cancel()

	gracePeriod := time.Second * 10

	ids := map[string]int{}
	mut := sync.Mutex{}

	var pullErr error

	testErr := FinishOrAbandon(ctx, func() {
		pullErr = rec.Receive(ctx, func(_ context.Context, message broker.Message) {
			CountMessages(&mut, ids, message)
		})
	}, gracePeriod)
	VerifyResults(t, testErr, pullErr, iterator.ErrorToReturn, iterator.Messages, ids)
}

// defines an iterator that returns messages and ends in an error.
// Converts the iterator to a receiver and checks that all messages and the error are received.
func TestMessageIteratorToMessageReceiver_Error(t *testing.T) {
	t.Parallel()

	iterator := &inmem.MessageSpawnIterator{
		Messages:       nil,
		AmountToReturn: 100,
		Timeout:        time.Second / 10,
		ErrorToReturn:  errors.New("simulated error"),
	}

	// batching with batch size 1 because otherwise the messages in the last batch could be ignored when an error is returned.
	batched := brokerutil.MessageIteratorIntoBatchedMessageIterator(iterator,
		brokerutil.IntoBatchedMessageIteratorSettings{BatchSize: 1, Timeout: time.Second},
	)
	rec := brokerutil.IteratorIntoReceiver{
		Iterator: batched,
		Settings: brokerutil.IntoReceiverSettings{NumGoroutines: 1},
	}

	pullDuration := iterator.Timeout*time.Duration(iterator.AmountToReturn+1) + time.Second*5

	ctx, cancel := context.WithTimeout(context.Background(), pullDuration)
	defer cancel()

	gracePeriod := time.Second * 10

	ids := map[string]int{}
	mut := sync.Mutex{}

	var pullErr error

	testErr := FinishOrAbandon(ctx, func() {
		pullErr = rec.Receive(ctx, func(_ context.Context, message broker.Message) {
			CountMessages(&mut, ids, message)
		})
	}, gracePeriod)

	VerifyResults(t, testErr, pullErr, iterator.ErrorToReturn, iterator.Messages, ids)
}
