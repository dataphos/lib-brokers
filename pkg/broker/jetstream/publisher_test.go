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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/jetstream"
)

func TestNewPublisher(t *testing.T) {
	t.Parallel()

	url := fmt.Sprintf("nats://127.0.0.1:%d", TestPort)
	publisher, err := jetstream.NewPublisher(context.Background(), url, jetstream.DefaultPublisherSettings)
	if err != nil { //nolint:wsl //file is gofumt-ed
		t.Fatal("Error creating publisher: ", err)
	}
	defer publisher.Close()

	// Test publisher creation fail.
	url = "nohost:9092"
	_, err = jetstream.NewPublisher(context.Background(), url, jetstream.DefaultPublisherSettings)

	if err == nil {
		t.Fatal("An error is expected because broker address is false.")
	}
}

func TestPublishOneMessage(t *testing.T) {
	t.Parallel()

	topicName := "Test.PublishOneMessage"
	sURL := fmt.Sprintf("nats://127.0.0.1:%d", TestPort)
	message := broker.OutboundMessage{
		Key:        "key",
		Data:       []byte("A simple message."),
		Attributes: map[string]interface{}{"customKey": "customValue"},
	}

	publisher, _ := jetstream.NewPublisher(context.Background(), sURL, jetstream.DefaultPublisherSettings)
	defer publisher.Close()

	topic, _ := publisher.Topic(topicName)

	err := topic.Publish(context.Background(), message)
	if err != nil {
		t.Fatal("Error while publishing new message: ", err)
	}

	_, err = jsCtx.Subscribe("Test.PublishOneMessage", func(msg *nats.Msg) {
		err := msg.Ack()
		if err != nil {
			t.Fatal("Error while accepting message: ", err)
		}
		require.Equal(t, msg.Data, message.Data)
	})
	if err != nil {
		t.Fatal("Error while pulling messages: ", err)
	}
}

func TestPublishMoreMessages(t *testing.T) {
	t.Parallel()

	topicName := "Test.PublishMoreMessages"
	sURL := fmt.Sprintf("nats://127.0.0.1:%d", TestPort)

	messages := []broker.OutboundMessage{
		{
			Key:        "key1",
			Data:       []byte("First message."),
			Attributes: map[string]interface{}{"customKey": "customValue"},
		},
		{
			Key:        "key2",
			Data:       []byte("Second message."),
			Attributes: map[string]interface{}{"customKey": "customValue"},
		},
		{
			Key:        "key3",
			Data:       []byte("Third message."),
			Attributes: map[string]interface{}{"customKey": "customValue"},
		},
		{
			Key:        "key4",
			Data:       []byte("Fourth message."),
			Attributes: map[string]interface{}{"customKey": "customValue"},
		},
	}

	publisher, _ := jetstream.NewPublisher(context.Background(), sURL, jetstream.DefaultPublisherSettings)
	defer publisher.Close()

	topic, _ := publisher.Topic(topicName)

	for _, message := range messages {
		err := topic.Publish(context.Background(), message)
		if err != nil {
			t.Fatal("Error while publishing new message: ", err)
		}
	}

	counter := 0
	_, err := jsCtx.Subscribe("Test.PublishMoreMessages", func(msg *nats.Msg) {
		err := msg.Ack()
		if err != nil {
			t.Fatal("Error while accepting message: ", err)
		}
		require.Equal(t, msg.Data, messages[counter].Data)
		counter++
	})
	if err != nil { //nolint:wsl //file gofumt-ed
		t.Fatal("Error while pulling messages: ", err)
	}

	if counter > 4 {
		t.Fatalf("Expected 4 messages, got %d.", counter)
	}
}

func TestPublishMessageNoKey(t *testing.T) {
	t.Parallel()

	topicName := "Test.PublishMessageNoKey"
	sURL := fmt.Sprintf("nats://127.0.0.1:%d", TestPort)
	message := broker.OutboundMessage{
		Data:       []byte("Message without key."),
		Attributes: map[string]interface{}{"customKey": "customValue"},
	}

	publisher, _ := jetstream.NewPublisher(context.Background(), sURL, jetstream.DefaultPublisherSettings)
	defer publisher.Close()

	topic, err := publisher.Topic(topicName)
	if err != nil {
		t.Fatal("Error creating topic", err)
	}

	err = topic.Publish(context.Background(), message)
	if err != nil {
		t.Fatal("Error while publishing new message: ", err)
	}

	_, err = jsCtx.Subscribe("Test.PublishMessageNoKey", func(msg *nats.Msg) {
		err := msg.Ack()
		if err != nil {
			t.Fatal("Error while accepting message: ", err)
		}
		require.Equal(t, msg.Data, message.Data)
	})
	if err != nil {
		t.Fatal("Error while pulling messages: ", err)
	}
}

func TestPublishBatch(t *testing.T) {
	t.Parallel()

	topicName := "Test.PublishBatch"
	sURL := fmt.Sprintf("nats://127.0.0.1:%d", TestPort)

	numMsgs := 1000
	messages := make([]broker.OutboundMessage, 0, numMsgs)

	for i := 0; i < numMsgs; i++ {
		messages = append(messages, broker.OutboundMessage{
			Key:        "key",
			Data:       []byte(strconv.Itoa(i)),
			Attributes: nil,
		})
	}

	publisher, err := jetstream.NewPublisher(context.Background(), sURL, jetstream.DefaultPublisherSettings)
	if err != nil {
		t.Fatal("Error creating publisher", err)
	}

	defer publisher.Close()

	topic, _ := publisher.Topic(topicName)

	err = topic.BatchPublish(context.Background(), messages...)
	if err != nil {
		t.Fatal("Error while publishing message: ", err)
	}

	payloads := sync.Map{}

	_, err = jsCtx.Subscribe("Test.PublishBatch", func(msg *nats.Msg) {
		err := msg.Ack()
		if err != nil {
			t.Fatal("Error while accepting message: ", err)
		}
		payloads.Store(string(msg.Data), struct{}{})
	})
	if err != nil {
		t.Fatal("Error while pulling messages: ", err)
	}

	// wait for some time for all the published messages to be consumed. Alternatively, we could call NextMsg
	// until we receive all messages.
	time.Sleep(5 * time.Second)

	for _, msg := range messages {
		dataString := string(msg.Data)
		if _, found := payloads.Load(dataString); !found {
			t.Errorf("Message %s not received", dataString)
		}
	}
}
