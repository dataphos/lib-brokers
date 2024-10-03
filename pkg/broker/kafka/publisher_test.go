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

package kafka_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
)

func TestNewPublisher(t *testing.T) {
	// Test publisher creation success.
	publisher, err := kafka.NewPublisher(context.Background(), producerConfig, producerSettings)
	require.NoError(t, err, "Could not create publisher.")
	err = publisher.Close()
	require.NoError(t, err, "Could not close publisher.")

	// Test publisher creation fail.
	publisher, err = kafka.NewPublisher(
		context.Background(),
		kafka.ProducerConfig{BrokerAddr: "nohost:9092"},
		kafka.DefaultProducerSettings,
	)
	if err == nil {
		if publisher != nil {
			publisher.Close()
		}

		t.Fatal("An error was expected as the broker address is false.")
	}
}

func TestTopic_PublishOneMessage(t *testing.T) {
	topicName := "test.topic.publish.one.message"
	topic, closePublisher := topicFromNewPublisher(t, topicName)

	defer closePublisher()
	// Create mock message.
	message := broker.OutboundMessage{
		Key:        "key",
		Data:       []byte("A simple message."),
		Attributes: map[string]interface{}{"customKey": "customValue"},
	}
	ctx := context.Background()
	// Publish the message.
	require.NoError(t, topic.Publish(ctx, message), "Message could not be published.")
	// Pull message.
	records := pullMessages(ctx, t, topicName)
	if len(records) != 1 {
		t.Fatalf("Exactly one record was expected, but pulled %d.\n", len(records))
	}
	// Finally, compare message content and metadata.
	assertEqualAsReceived(t, message, records[0])
}

func TestTopic_PublishNoKey(t *testing.T) {
	topicName := "test.topic.publish.no.key"
	topic, closePublisher := topicFromNewPublisher(t, topicName)

	defer closePublisher()
	// Create mock message.
	message := broker.OutboundMessage{
		Data: []byte("A simple message with no key."),
	}
	ctx := context.Background()
	// Publish the message.
	require.NoError(t, topic.Publish(ctx, message), "Message could not be published.")
	// Pull message.
	records := pullMessages(ctx, t, topicName)
	if len(records) != 1 {
		t.Fatalf("Exactly one record was expected, but pulled %d.\n", len(records))
	}
	// Finally, compare message content and metadata.
	assertEqualAsReceived(t, message, records[0])
}

func TestTopic_PublishMultipleMessages(t *testing.T) {
	topicName := "test.topic.publish.multiple.messages"
	topic, closePublisher := topicFromNewPublisher(t, topicName)

	defer closePublisher()

	messages := []broker.OutboundMessage{
		{Data: []byte("Message number 0.")},
		{Data: []byte("Message number 1.")},
		{Data: []byte("Message number 2.")},
	}
	ctx := context.Background()
	err := topic.Publish(ctx, messages[0])
	require.NoError(t, err, "Could not publish message 0.")
	err = topic.Publish(ctx, messages[1])
	require.NoError(t, err, "Could not publish message 1.")
	err = topic.Publish(ctx, messages[2])
	require.NoError(t, err, "Could not publish message 2.")

	records := pullMessages(ctx, t, topicName)
	require.Equalf(t, 3, len(records), "Expected exactly 3 records, got %d.", len(records))

	for i, record := range records {
		assertEqualAsReceived(t, messages[i], record)
	}
}

func TestTopic_PublishBatch(t *testing.T) {
	topicName := "test.topic.publish.batch"
	topic, closePublisher := topicFromNewPublisher(t, topicName)

	defer closePublisher()

	numMsgs := 1000
	messages := make([]broker.OutboundMessage, 0, numMsgs)

	for i := 0; i < numMsgs; i++ {
		messages = append(messages, broker.OutboundMessage{
			Key:        "key",
			Data:       []byte(strconv.Itoa(i)),
			Attributes: nil,
		})
	}

	ctx := context.Background()

	err := topic.BatchPublish(ctx, messages...)
	if err != nil {
		t.Fatal("Error while publishing message: ", err)
	}

	records := pullMessages(ctx, t, topicName)
	require.Equalf(t, numMsgs, len(records), "Expected exactly %d records, got %d.", numMsgs, len(records))

	for i, record := range records {
		assertEqualAsReceived(t, messages[i], record)
	}
}

func TestTopic_PublishWithNilAttribute(t *testing.T) {
	topicName := "test.topic.publish.with.nil.attribute"
	topic, closePublisher := topicFromNewPublisher(t, topicName)

	defer closePublisher()

	// Create mock message with a nil attribute.
	message := broker.OutboundMessage{
		Data:       []byte("A simple message."),
		Attributes: map[string]interface{}{"customAttribute": nil},
	}

	ctx := context.Background()
	// Try to publish the message.
	require.Panics(t, func() {
		topic.Publish(ctx, message)
	}, "The publish was expected to panic as an attribute should not be nil.")
}

func topicFromNewPublisher(t *testing.T, topicName string) (broker.Topic, func() error) {
	t.Helper()

	publisher, err := kafka.NewPublisher(context.Background(), producerConfig, producerSettings)
	require.NoError(t, err, "Could not create publisher.")
	topic, err := publisher.Topic(topicName)
	require.NoError(t, err, "Could not create topic.")

	return topic, publisher.Close
}

func pullMessages(ctx context.Context, t *testing.T, topicName string) []*kgo.Record {
	t.Helper()

	// Initialize franz-go kafka client.
	groupID := topicName + ".group"
	kgoClient, err := kgo.NewClient( //nolint:contextcheck //cannot pass the context
		kgo.SeedBrokers(BrokerAddrs),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topicName),
	)
	require.NoError(t, err)

	defer kgoClient.Close() //nolint:contextcheck //cannot pass the context

	ctxWithTimeout, cancel := context.WithTimeout(ctx, TimeoutDuration)
	defer cancel()

	// Pull messages.
	fetches := kgoClient.PollFetches(ctxWithTimeout)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatal(errs, "Could not pull messages.")
	}

	return fetches.Records()
}

func assertEqualAsReceived(t *testing.T, message broker.OutboundMessage, record *kgo.Record) {
	t.Helper()

	var messageKey []byte
	// Empty key is treated differently.
	if message.Key != "" {
		messageKey = []byte(message.Key)
	}

	require.EqualValues(
		t,
		messageKey,
		record.Key,
		"Keys are not equal. Expected: %v received: %v",
		messageKey,
		record.Key,
	)
	require.EqualValues(
		t,
		message.Data,
		record.Value,
		"Payloads are not equal. Expected: %v received: %v",
		message.Data,
		record.Value,
	)
	// Fill a map with headers for later check.
	headerMap := make(map[string]interface{})
	for _, header := range record.Headers {
		headerMap[header.Key] = header.Value
		attribute := message.Attributes[header.Key]

		if attribute != nil {
			require.EqualValues(
				t,
				attribute,
				header.Value,
				"Attributes with key %v are not equal. Expected: %v received: %v",
				header.Key,
				attribute,
				header.Value,
			)
		}
	}
	// Check if all attributes are present in received headers.
	for k := range message.Attributes {
		_, ok := headerMap[k]
		require.True(t, ok)
	}
}
