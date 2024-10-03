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

package jetstream_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/dataphos/lib-brokers/pkg/broker/jetstream"
)

func TestNewIterator(t *testing.T) {
	t.Parallel()

	config := jetstream.IteratorConfig{
		URL:          fmt.Sprintf("nats://127.0.0.1:%d", TestPort),
		Subject:      "Test.Subject",
		ConsumerName: "ConsumerIt",
	}
	iterator, err := jetstream.NewIterator(config)
	if err != nil { //nolint:wsl //file gofumt-ed
		t.Fatal("Error creating iterator: ", err)
	}
	defer iterator.Close()

	config.URL = "nohost:9092"
	iterator, _ = jetstream.NewIterator(config)

	if iterator != nil {
		t.Fatal("An error is expected because broker address is false.")
	}
}

func TestNewBatchIterator(t *testing.T) {
	t.Parallel()

	iteratorConfig := jetstream.IteratorConfig{
		URL:          fmt.Sprintf("nats://127.0.0.1:%d", TestPort),
		Subject:      "Test.Subject",
		ConsumerName: "ConsumerBatchIt",
	}

	batchIterator, err := jetstream.NewBatchIterator(context.Background(), iteratorConfig, jetstream.DefaultBatchIteratorSettings)
	if err != nil {
		t.Fatal("Error creating new batch iterator: ", err)
	}
	defer batchIterator.Close()

	iteratorConfig.URL = "nohost:9092"
	batchIterator, _ = jetstream.NewBatchIterator(context.Background(), iteratorConfig, jetstream.DefaultBatchIteratorSettings)

	if batchIterator != nil {
		t.Fatal("An error is expected because broker address is false.")
	}
}

func TestPullMessage(t *testing.T) {
	t.Parallel()

	iteratorConfig := jetstream.IteratorConfig{
		URL:          fmt.Sprintf("nats://127.0.0.1:%d", TestPort),
		Subject:      "Test.PullMessage",
		ConsumerName: "ConsumerMessage",
	}

	iterator, err := jetstream.NewIterator(iteratorConfig)
	if err != nil {
		t.Fatal("Error creating iterator", err)
	}
	defer iterator.Close()

	message := &nats.Msg{Data: []byte("Testing pull message."), Subject: iteratorConfig.Subject}

	_, err = jsCtx.Publish(iteratorConfig.Subject, message.Data)
	if err != nil {
		t.Fatal("Error publishing new message ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nextMessage, err := iterator.NextMessage(ctx)
	if err != nil {
		t.Fatal("Error pulling new message ", err)
	}

	require.Equal(t, message.Data, nextMessage.Data)
}

func TestIteratorDifferentSubjects(t *testing.T) {
	t.Parallel()

	iteratorConfig := jetstream.IteratorConfig{
		URL:          fmt.Sprintf("nats://127.0.0.1:%d", TestPort),
		Subject:      "Test.DifferentSubjects.*",
		ConsumerName: "ConsumerDiffSubj",
	}

	iterator, err := jetstream.NewIterator(iteratorConfig)
	if err != nil {
		t.Fatal("Error creating iterator", err)
	}
	defer iterator.Close()

	message := &nats.Msg{Data: []byte("Testing pull message on first subject."), Subject: "Test.DifferentSubjects.FirstSubject"}

	_, err = jsCtx.Publish(message.Subject, message.Data)
	if err != nil {
		t.Fatal("Error publishing new message ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nextMessage, err := iterator.NextMessage(ctx)
	if err != nil {
		t.Fatal("Error pulling new message ", err)
	}

	require.Equal(t, message.Data, nextMessage.Data)

	message = &nats.Msg{Data: []byte("Testing pull message on second subject."), Subject: "Test.DifferentSubjects.SecondSubject"}

	_, err = jsCtx.Publish(message.Subject, message.Data)
	if err != nil {
		t.Fatal("Error publishing new message ", err)
	}

	nextMessage, err = iterator.NextMessage(context.Background())
	if err != nil {
		t.Fatal("Error pulling new message ", err)
	}

	require.Equal(t, message.Data, nextMessage.Data)
}

func TestPullBatch(t *testing.T) {
	t.Parallel()

	iteratorConfig := jetstream.IteratorConfig{
		URL:          fmt.Sprintf("nats://127.0.0.1:%d", TestPort),
		Subject:      "Test.PullBatch",
		ConsumerName: "ConsumerBatch",
	}

	batchIterator, err := jetstream.NewBatchIterator(context.Background(), iteratorConfig, jetstream.DefaultBatchIteratorSettings)
	if err != nil {
		t.Fatal("Error creating new batch iterator: ", err)
	}
	defer batchIterator.Close()

	message1 := &nats.Msg{Data: []byte("First message."), Subject: iteratorConfig.Subject}
	message2 := &nats.Msg{Data: []byte("Second message."), Subject: iteratorConfig.Subject}
	message3 := &nats.Msg{Data: []byte("Third message."), Subject: iteratorConfig.Subject}
	message4 := &nats.Msg{Data: []byte("Fourth message."), Subject: iteratorConfig.Subject}

	messages := []nats.Msg{*message1, *message2, *message3, *message4}

	for _, message := range messages {
		_, err := jsCtx.Publish(iteratorConfig.Subject, message.Data)
		if err != nil {
			t.Fatal("Error publishing new message ", err)
		}
	}

	messageBatch, err := batchIterator.NextBatch(context.Background())
	if err != nil {
		t.Fatal("Error pulling batch: ", err)
	}

	if len(messageBatch) != len(messages) {
		for _, message := range messageBatch {
			fmt.Println(message.Data)
		}

		t.Fatalf("Expected %d messages, got %d.", len(messages), len(messageBatch))
	}

	for index, message := range messageBatch {
		require.Equal(t, message.Data, messages[index].Data)
	}
}

func TestBatchIteratorDifferentSubjects(t *testing.T) {
	t.Parallel()

	iteratorConfig := jetstream.IteratorConfig{
		URL:          fmt.Sprintf("nats://127.0.0.1:%d", TestPort),
		Subject:      fmt.Sprintf("%s.*", "Test.DifferentSubjectsBatch"),
		ConsumerName: "ConsumerDiffSubjBatch",
	}
	subject1, subject2 := "Test.DifferentSubjectsBatch.FirstSubject", "Test.DifferentSubjectsBatch.SecondSubject"

	batchIterator, err := jetstream.NewBatchIterator(context.Background(), iteratorConfig, jetstream.DefaultBatchIteratorSettings)
	if err != nil {
		t.Fatal("Error creating new batch iterator: ", err)
	}
	defer batchIterator.Close()

	message1 := &nats.Msg{Data: []byte("First message on first subject."), Subject: subject1}
	message2 := &nats.Msg{Data: []byte("Second message on first subject."), Subject: subject1}
	message3 := &nats.Msg{Data: []byte("Third message on first subject."), Subject: subject1}
	message4 := &nats.Msg{Data: []byte("Fourth message on first subject."), Subject: subject1}

	messages := []nats.Msg{*message1, *message2, *message3, *message4}

	for _, message := range messages {
		_, err := jsCtx.Publish(subject1, message.Data)
		if err != nil {
			t.Fatal("Error publishing new message ", err)
		}
	}

	messageBatch, err := batchIterator.NextBatch(context.Background())
	if err != nil {
		t.Fatal("Error pulling batch: ", err)
	}

	if len(messageBatch) != len(messages) {
		for _, message := range messageBatch {
			fmt.Println(message.Data)
		}

		t.Fatalf("Expected %d messages, got %d.", len(messages), len(messageBatch))
	}

	for index, message := range messageBatch {
		require.Equal(t, message.Data, messages[index].Data)
	}

	message1 = &nats.Msg{Data: []byte("First message on second subject."), Subject: subject2}
	message2 = &nats.Msg{Data: []byte("Second message on second subject."), Subject: subject2}
	message3 = &nats.Msg{Data: []byte("Third message on second subject."), Subject: subject2}
	message4 = &nats.Msg{Data: []byte("Fourth message on second subject."), Subject: subject2}

	messages = []nats.Msg{*message1, *message2, *message3, *message4}

	for _, message := range messages {
		_, err := jsCtx.Publish(subject2, message.Data)
		if err != nil {
			t.Fatal("Error publishing new message ", err)
		}
	}

	messageBatch, err = batchIterator.NextBatch(context.Background())
	if err != nil {
		t.Fatal("Error pulling batch: ", err)
	}

	if len(messageBatch) != len(messages) {
		for _, message := range messageBatch {
			fmt.Println(message.Data)
		}

		t.Fatalf("Expected %d messages, got %d.", len(messages), len(messageBatch))
	}

	for index, message := range messageBatch {
		require.Equal(t, message.Data, messages[index].Data)
	}
}
