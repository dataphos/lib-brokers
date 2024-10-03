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

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/require"

	"github.com/dataphos/lib-brokers/pkg/broker"
	p "github.com/dataphos/lib-brokers/pkg/broker/pulsar"
)

var publisherSettings p.PublisherSettings

func TestPublisher_Topic(t *testing.T) {
	topicName := "test.publisher.topic"

	// try to connect to broker, no error is expected.
	publisher, err := p.NewPublisher(p.PublisherConfig{
		ServiceURL: ServiceURL,
	}, publisherSettings)
	require.NoError(t, err, "Could not create publisher")
	_, err = publisher.Topic(topicName)
	require.NoError(t, err, "could not connect to broker: %v", err)
	require.NoError(t, publisher.Close(), "Could not close publisher")

	// try to connect to non-existing broker, an error is expected.
	maxReconnectToBroker := uint(1)
	settings := publisherSettings
	settings.MaxReconnectToBroker = &maxReconnectToBroker

	publisher, _ = p.NewPublisher(p.PublisherConfig{
		ServiceURL: "pulsar://nohost:6650",
	}, settings)
	_, err = publisher.Topic(topicName)
	require.Error(t, err, "an error was expected as broker at nohost:6650 doesn't exist")
	require.NoError(t, publisher.Close(), "Could not close publisher")
}

func TestTopic_PublishOneMessage(t *testing.T) {
	topicName := "test.topic.publish.one.message"
	subscriptionName := "test.subscription.publish.one.message"

	// A subscription must be created before a message is sent.
	consumer, closeConsumer := createSubscriptionForTopic(t, topicName, subscriptionName)
	defer closeConsumer()

	topic, closePublisher := topicFromNewPublisher(t, topicName)
	defer closePublisher()

	// Create mock message.
	message := broker.OutboundMessage{
		Key:        "key",
		Data:       []byte("A simple message."),
		Attributes: map[string]interface{}{"customKey": "customValue"},
	}

	// Publish the message.
	require.NoError(t, topic.Publish(context.Background(), message), "Message could not be published.")

	// Pull the message.
	receivedMessage := pullMessage(context.Background(), t, consumer)
	assertEqualAsReceived(t, message, receivedMessage)
}

func TestTopic_PublishNoKey(t *testing.T) {
	topicName := "test.topic.publish.no.key"
	subscriptionName := "test.subscription.publish.no.key"

	// A subscription must be created before a message is sent.
	consumer, closeConsumer := createSubscriptionForTopic(t, topicName, subscriptionName)
	defer closeConsumer()

	topic, closePublisher := topicFromNewPublisher(t, topicName)
	defer closePublisher()

	// Create mock message.
	message := broker.OutboundMessage{Data: []byte("A simple message.")}

	// Publish the message.
	require.NoError(t, topic.Publish(context.Background(), message), "Message could not be published.")

	// Pull the message.
	receivedMessage := pullMessage(context.Background(), t, consumer)
	assertEqualAsReceived(t, message, receivedMessage)
}

func TestTopic_PublishMultipleMessages(t *testing.T) {
	topicName := "test.topic.publish.multiple.messages"
	subscriptionName := "test.subscription.publish.multiple.messages"

	// A subscription must be created before a message is sent.
	consumer, closeConsumer := createSubscriptionForTopic(t, topicName, subscriptionName)
	defer closeConsumer()

	topic, closePublisher := topicFromNewPublisher(t, topicName)
	defer closePublisher()

	messages := []broker.OutboundMessage{
		{Data: []byte("Message number 0.")},
		{Data: []byte("Message number 1."), Attributes: map[string]interface{}{"k1": "v1", "k2": "v2"}},
		{Data: []byte("Message number 2.")},
	}
	ctx := context.Background()
	require.NoError(t, topic.Publish(ctx, messages[0]), "Could not publish message 0.")
	require.NoError(t, topic.Publish(ctx, messages[1]), "Could not publish message 1.")
	require.NoError(t, topic.Publish(ctx, messages[2]), "Could not publish message 2.")

	records := pullNMessages(ctx, t, consumer, 3)
	require.Equalf(t, 3, len(records), "Expected exactly 3 records, got %d.", len(records))

	for i, record := range records {
		assertEqualAsReceived(t, messages[i], record)
	}
}

func TestTopic_PublishWithNilAttribute(t *testing.T) {
	topicName := "test.topic.publish.with.nil.attribute"

	topic, closePublisher := topicFromNewPublisher(t, topicName)
	defer closePublisher()

	message := broker.OutboundMessage{
		Key:        "key",
		Data:       []byte("Land of Confusion"),
		Attributes: map[string]interface{}{"Attribute": nil},
	}

	require.Panics(t, func() {
		topic.Publish(context.Background(), message)
	}, "The publish was expected to panic as an attribute should not be nil.")
}

// topicFromNewPublisher creates a broker.Publisher from which it creates a broker.Topic and a closing function.
func topicFromNewPublisher(t *testing.T, topicName string) (broker.Topic, func() error) {
	t.Helper()

	publisher, err := p.NewPublisher(p.PublisherConfig{ServiceURL: ServiceURL}, p.DefaultPublisherSettings)
	require.NoError(t, err, "could not create publisher: %v", err)

	topic, err := publisher.Topic(topicName)
	require.NoError(t, err, "could not get topic: %v", err)

	return topic, publisher.Close
}

// createSubscriptionForTopic subscribes a pulsar.Consumer to the given topicName with the given subscriptionName.
func createSubscriptionForTopic( //nolint:ireturn //have to return interface
	t *testing.T,
	topicName,
	subscriptionName string,
) (pulsar.Consumer, func()) {
	t.Helper()

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: ServiceURL})
	require.NoError(t, err, "could not create pulsar client: %v", err)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{Topic: topicName, SubscriptionName: subscriptionName})
	require.NoError(t, err, "could not create subscription: %v", err)

	return consumer, consumer.Close
}

// pullNMessages pulls n messages using the given consumer.
//
// It fails if a message cannot be pulled within the TimeoutDuration.
func pullNMessages(ctx context.Context, t *testing.T, consumer pulsar.Consumer, n int) []pulsar.Message { //nolint:varnamelen //intentionally short
	t.Helper()

	// Pull messages.
	messages := make([]pulsar.Message, n)

	for i := 0; i < n; i++ { //nolint:varnamelen //intentionally short
		ctxWithTimeout, cancel := context.WithTimeout(ctx, TimeoutDuration)

		message, err := consumer.Receive(ctxWithTimeout)
		require.NoError(t, err, "could not pull message: %v", err)
		cancel()

		messages[i] = message
	}

	return messages
}

// pullMessage pulls a message using the given consumer.
//
// It fails if the message cannot be pulled within the TimeoutDuration.
func pullMessage(ctx context.Context, t *testing.T, consumer pulsar.Consumer) pulsar.Message { //nolint:ireturn //have to return interface
	t.Helper()

	return pullNMessages(ctx, t, consumer, 1)[0]
}

// assertEqualAsReceived checks that all relevant fields of the message sent is equal to the received message.
func assertEqualAsReceived(t *testing.T, message broker.OutboundMessage, received pulsar.Message) {
	t.Helper()

	require.EqualValues(t, message.Key, received.Key())
	require.EqualValues(t, message.Data, received.Payload())

	for k, v := range message.Attributes {
		receivedAttributeValue := received.Properties()[k]
		require.NotNil(t, receivedAttributeValue, "Attribute with key %s not present in received")
		require.EqualValues(t, v, receivedAttributeValue)
	}
}
