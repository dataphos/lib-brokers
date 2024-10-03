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

package servicebus_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/require"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/servicebus"
)

func TestNewPublisher(t *testing.T) {
	if connectionString == "" {
		t.Skip("Connection string not set, skipping unit test")
	}

	publisher, err := servicebus.NewPublisher(connectionString)
	require.NoError(t, err, "Could not create publisher.")

	defer func() {
		require.NoError(t, publisher.Close(), "Error closing publisher.")
	}()

	require.NotNil(t, publisher, "Expected non-nil publisher, got nil")
}

func TestPublishOne(t *testing.T) {
	if connectionString == "" {
		t.Skip("Connection string not set, skipping unit test")
	}

	publisher, err := servicebus.NewPublisher(connectionString)
	require.NoError(t, err, "Could not create publisher.")

	defer func() {
		require.NoError(t, publisher.Close(), "Error closing publisher.")
	}()

	topic, err := publisher.Topic(topicID)
	require.NoError(t, err, "Error creating topic %s.", topicID)

	message := broker.OutboundMessage{
		Data:       []byte("test message"),
		Attributes: map[string]interface{}{"key": "value"},
	}

	err = topic.Publish(context.Background(), message)
	require.NoError(t, err, "Error publishing message.")

	receiver, err := publisher.Client.NewReceiverForSubscription(topicID, subscriptionID, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete,
	})
	require.NoError(t, err, "Error creating receiver for subscription %s.", subscriptionID)

	defer func() {
		require.NoError(t, receiver.Close(context.Background()), "Error closing receiver.")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := receiver.ReceiveMessages(ctx, 1, nil)
	require.NoError(t, err, "Error receiving message.")

	if len(msg) != 1 {
		t.Errorf("Expected 1 message, received %d", len(msg))
	}

	receivedMessage := msg[0]

	require.Equal(t, message.Data, receivedMessage.Body, "Received message data does not match the original")
}

func TestBatchPublish(t *testing.T) {
	if connectionString == "" {
		t.Skip("SERVICEBUS_CONNECTION_STRING not set, skipping unit test")
	}

	publisher, err := servicebus.NewPublisher(connectionString)
	require.NoError(t, err, "Could not create publisher.")

	defer func() {
		require.NoError(t, publisher.Close(), "Error closing publisher.")
	}()

	topic, err := publisher.Topic(topicID)
	require.NoError(t, err, "Error creating topic %s.", topicID)

	messages := []broker.OutboundMessage{
		{Data: []byte("message 1"), Attributes: map[string]interface{}{"key1": "value1"}},
		{Data: []byte("message 2"), Attributes: map[string]interface{}{"key2": "value2"}},
		{Data: []byte("message 3"), Attributes: map[string]interface{}{"key3": "value3"}},
	}

	err = topic.BatchPublish(context.Background(), messages...)
	require.NoError(t, err, "Error batch publishing messages.")

	receiver, err := publisher.Client.NewReceiverForSubscription(topicID, subscriptionID, &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete,
	})
	require.NoError(t, err, "Error creating receiver for subscription %s.", subscriptionID)

	defer func() {
		require.NoError(t, receiver.Close(context.Background()), "Error closing receiver.")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := receiver.ReceiveMessages(ctx, len(messages), nil)
	require.NoError(t, err, "Error receiving message.")

	require.Equal(t, len(results), len(messages))

	var received bool
	for _, message := range messages {
		received = false

		for _, result := range results {
			if reflect.DeepEqual(message.Data, result.Body) {
				received = true
			}
		}

		if !received {
			t.Fatalf("Message [%s] not received", string(message.Data))
		}
	}
}
