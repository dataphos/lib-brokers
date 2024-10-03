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

package pulsar_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/require"

	"github.com/dataphos/lib-brokers/pkg/broker"
	p "github.com/dataphos/lib-brokers/pkg/broker/pulsar"
)

func TestNewIterator(t *testing.T) {
	topicName := "test.topic.new.iterator"
	subscriptionName := "test.subscription.new.iterator"

	// Test iterator creation success.
	iterator, err := p.NewIterator(p.IteratorConfig{
		ServiceURL:   ServiceURL,
		Topic:        topicName,
		Subscription: subscriptionName,
	}, p.DefaultIteratorSettings)
	require.NoError(t, err, "No error was expected when creating the iterator.")
	require.NoError(t, iterator.Close())

	// Test iterator creation fail.
	iterator, err = p.NewIterator(p.IteratorConfig{
		ServiceURL:   "pulsar://nohost:6650",
		Topic:        topicName,
		Subscription: subscriptionName,
	}, p.DefaultIteratorSettings)
	require.Error(t, err, "an error was expected as broker at nohost:6650 doesn't exist")

	if iterator != nil {
		iterator.Close()
	}
}

func TestIterator_NextMessageOneMessage(t *testing.T) {
	topicName := "test.topic.next.message.one.message"
	subscriptionName := "test.subscription.next.message.one.message"

	// Shorten NackRedeliveryDelay to test if nacked messages can be pulled again.
	settings := p.DefaultIteratorSettings
	settings.NackRedeliveryDelay = 1 * time.Second

	// First, create the iterator so a subscription is created and messages are not lost.
	iterator, err := p.NewIterator(p.IteratorConfig{
		ServiceURL:   ServiceURL,
		Topic:        topicName,
		Subscription: subscriptionName,
	}, settings)
	require.NoError(t, err, "couldn't create iterator: %v", err)

	defer iterator.Close()

	sentMessage := pulsar.ProducerMessage{
		Payload:     []byte("Payload of the message"),
		Key:         "key",
		OrderingKey: "orderingKey",
		Properties:  map[string]string{"k1": "v1", "k2": "v2"},
	}
	publishMessages(context.Background(), t, topicName, &sentMessage)

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	receivedMessage, err := iterator.NextMessage(ctxWithTimeout)
	require.NoError(t, err, "could not pull message: %v", err)

	assertEqualAsSent(t, sentMessage, receivedMessage)

	// Remember message ID to check if redelivery works.
	msgID := receivedMessage.ID
	// Say that processing failed and long enough so that the message can be redelivered.
	receivedMessage.Nack()
	time.Sleep(settings.NackRedeliveryDelay)

	ctxWithTimeout, cancel = context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()
	// PUll a message and check it is the same one as before.
	receivedMessage, err = iterator.NextMessage(ctxWithTimeout)
	require.NoError(t, err, "could not pull message: %v", err)
	require.EqualValues(t, msgID, receivedMessage.ID)
	assertEqualAsSent(t, sentMessage, receivedMessage)
}

func TestIterator_NextMessageNoKey(t *testing.T) {
	topicName := "test.topic.next.message.no.key"
	subscriptionName := "test.subscription.next.message.no.key"

	// First, create the iterator so a subscription is created and messages are not lost.
	iterator, err := p.NewIterator(p.IteratorConfig{
		ServiceURL:   ServiceURL,
		Topic:        topicName,
		Subscription: subscriptionName,
	}, p.DefaultIteratorSettings)
	require.NoError(t, err, "couldn't create iterator: %v", err)

	defer iterator.Close()

	sentMessage := pulsar.ProducerMessage{Payload: []byte("Payload.")}
	publishMessages(context.Background(), t, topicName, &sentMessage)

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	receivedMessage, err := iterator.NextMessage(ctxWithTimeout)
	require.NoError(t, err, "could not pull message: %v", err)

	assertEqualAsSent(t, sentMessage, receivedMessage)
}

func TestIterator_NextMessageMultipleMessages(t *testing.T) {
	topicName := "test.topic.next.message.multiple.messages"
	subscriptionName := "test.subscription.next.message.multiple.messages"

	// Shorten NackRedeliveryDelay to test if nacked messages can be pulled again.
	settings := p.DefaultIteratorSettings
	settings.NackRedeliveryDelay = 1 * time.Second

	// First, create the iterator so a subscription is created and messages are not lost.
	iterator, err := p.NewIterator(p.IteratorConfig{
		ServiceURL:   ServiceURL,
		Topic:        topicName,
		Subscription: subscriptionName,
	}, settings)
	require.NoError(t, err, "couldn't create iterator: %v", err)

	defer iterator.Close()

	sentMessage1 := pulsar.ProducerMessage{Key: "key", Payload: []byte("Payload of message1.")}
	sentMessage2 := pulsar.ProducerMessage{Key: "key", Payload: []byte("Payload of message2.")}
	sentMessage3 := pulsar.ProducerMessage{Key: "key", Payload: []byte("Payload of message3.")}
	publishMessages(context.Background(), t, topicName, &sentMessage1, &sentMessage2, &sentMessage3)

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 3*TimeoutDuration)
	defer cancel()

	receivedMessage1, err := iterator.NextMessage(ctxWithTimeout)
	require.NoError(t, err, "could not pull message: %v", err)
	receivedMessage2, err := iterator.NextMessage(ctxWithTimeout)
	require.NoError(t, err, "could not pull message: %v", err)
	receivedMessage3, err := iterator.NextMessage(ctxWithTimeout)
	require.NoError(t, err, "could not pull message: %v", err)

	assertEqualAsSent(t, sentMessage1, receivedMessage1)
	assertEqualAsSent(t, sentMessage2, receivedMessage2)
	assertEqualAsSent(t, sentMessage3, receivedMessage3)

	// Remember message ID to check if redelivery works.
	msgID2 := receivedMessage2.ID
	// Say that processing of second message failed and sleep long enough so that the message can be redelivered.
	receivedMessage1.Ack()
	receivedMessage2.Nack()
	receivedMessage3.Ack()
	time.Sleep(settings.NackRedeliveryDelay)

	ctxWithTimeout, cancel = context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()
	// PUll a message and check it is the same one as before.
	receivedMessage2, err = iterator.NextMessage(ctxWithTimeout)
	require.NoError(t, err, "could not pull message: %v", err)
	require.EqualValues(t, msgID2, receivedMessage2.ID)
	assertEqualAsSent(t, sentMessage2, receivedMessage2)
}

func publishMessages(ctx context.Context, t *testing.T, topicName string, messages ...*pulsar.ProducerMessage) {
	t.Helper()

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: ServiceURL})
	require.NoError(t, err, "could not create pulsar client: %v", err)

	publisher, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topicName})
	require.NoError(t, err, "could not create pulsar publisher: %v", err)

	// send all messages synchronously.
	for _, message := range messages {
		_, err := publisher.Send(ctx, message)
		require.NoError(t, err, "could not publish message: %v", err)
	}
}

func assertEqualAsSent(t *testing.T, sent pulsar.ProducerMessage, received broker.Message) {
	t.Helper()

	require.EqualValues(t, sent.Key, received.Key)
	require.EqualValues(t, sent.Payload, received.Data)

	for k, v := range sent.Properties {
		receivedAttribute := received.Attributes[k]
		attribute, ok := receivedAttribute.(string)
		require.True(t, ok, "attribute %v could not be ast to string", receivedAttribute)
		require.EqualValues(t, v, attribute)
	}
}
