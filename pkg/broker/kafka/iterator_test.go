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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
	"github.com/dataphos/lib-brokers/pkg/brokerutil"
)

func TestNewIterator(t *testing.T) {
	topicName := "test.new.iterator"
	groupID := topicName + ".group"
	// Test iterator creation success.
	iterator, err := kafka.NewIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
		TLS:        nil,
	}, consumerSettings)
	require.NoError(t, err, "Could not create consumer.")
	iterator.Close()

	// Test iterator creation fail.
	iterator, err = kafka.NewIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: "nohost:9092",
		GroupID:    groupID,
		Topic:      topicName,
		TLS:        nil,
	}, consumerSettings)
	if err == nil {
		if iterator != nil {
			iterator.Close()
		}

		t.Fatal("An error was expected as the broker address is false.")
	}
}

func TestIterator_NextRecordOneMessage(t *testing.T) {
	topicName := "test.iterator.next.record.one.message"
	groupID := topicName + ".group"
	sentRecord := kgo.Record{
		Key:   []byte("key"),
		Value: []byte("Simple message"),
		Headers: []kgo.RecordHeader{
			{"customKey", []byte("customValue")},
			{"anotherCustomKey", []byte{4, 2}},
		},
		Topic: topicName,
	}

	ctx := context.Background()
	// Publish the message.
	publishMessages(ctx, t, &sentRecord)

	// Create consumer.
	iterator, err := kafka.NewIterator(ctx, kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}, consumerSettings)
	require.NoError(t, err, "Could not create iterator.")

	defer iterator.Close()

	// Consume the message.
	ctxWithCancel, cancel := context.WithTimeout(ctx, TimeoutDuration)
	defer cancel()

	receivedRecord, err := iterator.NextRecord(ctxWithCancel)
	require.NoError(t, err, "Could not pull the message.")

	// Finally compare the messages.
	assertEqualAsSent(t, sentRecord, receivedRecord)
	// Check if committing works as expected.
	checkNumberOfCommitted(ctx, t, topicName, groupID, int32(receivedRecord.Partition), -1)
	receivedRecord.Ack()
	checkNumberOfCommitted(ctx, t, topicName, groupID, int32(receivedRecord.Partition), 1)
}

func TestIterator_NextRecordNoKey(t *testing.T) {
	topicName := "test.iterator.next.record.no.key"
	groupID := topicName + ".group"
	sentRecord := kgo.Record{
		Value: []byte("Simple message with no key."),
		Topic: topicName,
	}

	ctx := context.Background()
	// Publish the message.
	publishMessages(ctx, t, &sentRecord)

	// Create consumer.
	iterator, err := kafka.NewIterator(ctx, kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}, consumerSettings)
	require.NoError(t, err, "Could not create iterator.")

	defer iterator.Close()

	// Consume the message.
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), TimeoutDuration)

	defer cancel()

	receivedRecord, err := iterator.NextRecord(ctxWithTimeout)
	require.NoError(t, err, "Could not pull the message.")

	// Finally compare the messages.
	assertEqualAsSent(t, sentRecord, receivedRecord)
	receivedRecord.Ack()
}

func TestIterator_NextRecordMultipleMessages(t *testing.T) {
	topicName := "test.iterator.next.record.multiple.messages"
	groupID := topicName + ".group"
	sentRecords := []*kgo.Record{
		{Key: []byte("key0"), Value: []byte("Message number 0."), Topic: topicName},
		{Key: []byte("key1"), Value: []byte("Message number 1."), Topic: topicName},
		{Key: []byte("key2"), Value: []byte("Message number 2."), Topic: topicName},
	}

	ctx := context.Background()
	// Publish the message.
	publishMessages(ctx, t, sentRecords...)

	// Create consumer.
	iterator, err := kafka.NewIterator(ctx, kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}, consumerSettings)
	require.NoError(t, err, "Could not create iterator.")

	defer iterator.Close()

	ctxWithCancel, cancel := context.WithTimeout(ctx, TimeoutDuration)

	defer cancel()
	// Consume the messages.
	receivedRecords := make([]broker.Record, len(sentRecords))

	for i := 0; i < len(sentRecords); i++ { //nolint:varnamelen //intentionally short
		record, err := iterator.NextRecord(ctxWithCancel)
		require.NoError(t, err, "Could not pull message.")

		receivedRecords[i] = record
		// Check that it was -1 before first commit.
		if i == 0 {
			checkNumberOfCommitted(ctx, t, topicName, groupID, int32(record.Partition), int64(-1))
		}

		record.Ack()
		// Check that the message is committed.
		checkNumberOfCommitted(ctx, t, topicName, groupID, int32(record.Partition), int64(i+1))
	}

	// Finally compare the messages.
	for i, sentRecord := range sentRecords {
		assertEqualAsSent(t, *sentRecord, receivedRecords[i])
	}
}

func TestNewBatchIterator(t *testing.T) {
	topicName := "test.new.batch.iterator"
	groupID := topicName + ".group"
	// Test batch iterator creation success.
	client, err := kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}, batchConsumerSettings)
	require.NoError(t, err, "BatchConsumer could not be created.")

	defer client.Close()

	// Test batch iterator creation fail.
	client, err = kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: "nohost:9092",
		GroupID:    groupID,
		Topic:      topicName,
	}, batchConsumerSettings)
	if err == nil {
		if client != nil {
			client.Close()
		}

		t.Fatal("An error was expected as the broker address is false.")
	}
}

func TestBatchIterator_NextBatch(t *testing.T) {
	topicName := "test.batch.iterator.next.batch"
	groupID := topicName + ".group"
	partitionNum := 2

	topicClient, err := kgo.NewClient(
		kgo.SeedBrokers(BrokerAddrs),
	)
	require.NoError(t, err, "Could not create franz go client.")

	defer topicClient.Close()
	adminClient := kadm.NewClient(topicClient)
	_, err = adminClient.CreateTopics(context.Background(), int32(partitionNum), 1, map[string]*string{}, topicName)
	require.NoError(t, err, "Could not create topic")
	// Publish a batch of mock messages.
	sentRecords := []*kgo.Record{
		{
			Value: []byte("Message number 0."),
			Topic: topicName,
		},
		{
			Value: []byte("Message number 1."),
			Topic: topicName,
		},
	}
	publishMessages(context.Background(), t, sentRecords...)

	// Create the batch iterator.
	client, err := kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}, batchConsumerSettings)
	require.NoError(t, err, "BatchConsumer could not be created.")

	defer client.Close()

	// Pull the batch.
	ctx := context.Background()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, TimeoutDuration)

	defer cancel()

	receivedRecords, err := client.NextBatch(ctxWithTimeout)
	require.NoError(t, err, "Could not pull batch of messages.")
	// Check if equal number of messages was received.
	require.Equal(t, len(sentRecords), len(receivedRecords))

	// check if the content is equal.
	for i, receivedRecord := range receivedRecords {
		assertEqualAsSent2(t, *sentRecords[i], receivedRecord)
	}

	// Check if commit works as expected.
	brokerutil.CommitHighestOffsets(receivedRecords)
	expectedOffset := len(receivedRecords)
	checkNumberOfCommittedAllPartitions(ctx, t, topicName, groupID, int32(partitionNum), int64(expectedOffset))
}

// Test if BatchIterator pulls expected number of messages in a batch,
// if committing works as expected and if an uncommitted batch can be pulled again.
func TestBatchIterator_NextBatchTwoBatches(t *testing.T) {
	topicName := "test.batch.iterator.next.batch.two.batches"
	groupID := topicName + ".group"
	// Publish a batch of mock messages.
	partitionNum := 2

	topicClient, err := kgo.NewClient(
		kgo.SeedBrokers(BrokerAddrs),
	)
	require.NoError(t, err, "Could not create franz go client.")

	defer topicClient.Close()

	adminClient := kadm.NewClient(topicClient)
	_, err = adminClient.CreateTopics(context.Background(), int32(partitionNum), 1, map[string]*string{}, topicName)
	require.NoError(t, err, "Could not create topic")

	numSent := batchConsumerSettings.MaxPollRecords + 1
	sentRecords := make([]*kgo.Record, numSent)

	for i := 0; i < numSent; i++ {
		sentRecords[i] = &kgo.Record{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("Message number %d.", i)),
			Topic: topicName,
		}
	}
	publishMessages(context.Background(), t, sentRecords...)

	// Create the batch iterator.
	client, err := kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}, batchConsumerSettings)
	require.NoError(t, err, "BatchIterator could not be created.")

	defer client.Close()

	// Pull two batches.
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	batch1, err := client.NextBatch(ctxWithTimeout)
	require.NoError(t, err, "Could not pull first batch.")
	require.Greater(t, len(batch1), 0, "Expected to pull more than 0 messages.")

	ctxWithTimeout, cancel = context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	// Commit the first batch.
	ctx := context.Background()
	checkNumberOfCommitted(ctx, t, topicName, groupID, int32(batch1[0].Partition), int64(-1))
	brokerutil.CommitHighestOffsets(batch1)
	checkNumberOfCommittedAllPartitions(ctx, t, topicName, groupID, int32(partitionNum), int64(len(batch1)))

	batch2, err := client.NextBatch(ctxWithTimeout)
	require.NoError(t, err, "Could not pull second batch.")
	require.True(
		t,
		len(batch1) <= batchConsumerSettings.MaxPollRecords && len(batch1) > 0,
		"Expected more than 0 messages and a maximum of %d messages, got %d.",
		batchConsumerSettings.MaxPollRecords,
		len(batch1),
	)
	require.True(
		t,
		len(batch2) <= batchConsumerSettings.MaxPollRecords && len(batch2) > 0,
		"Expected more than 0 messages and a maximum of %d messages, got %d.",
		batchConsumerSettings.MaxPollRecords,
		len(batch2),
	)

	// Put the records back together.
	receivedRecords := make([]broker.Record, 0)
	receivedRecords = append(receivedRecords, batch1...)
	receivedRecords = append(receivedRecords, batch2...)

	// check if the content is equal.
	for _, receivedRecord := range receivedRecords {
		for _, sentRecord := range sentRecords {
			if string(sentRecord.Key) == receivedRecord.Key {
				assertEqualAsSent2(t, *sentRecord, receivedRecord)
			}
		}
	}

	// Act as if the client went down and connect again.
	client.Close()
	// Wait until the broker says the client is closed, meaning the group is empty.
	maxTries := 5
	waitTime := 50 * time.Millisecond

	var numberOfMembers int

	for i := 0; i < maxTries; i++ {
		numberOfMembers = getNumberOfMembers(context.Background(), t, groupID)
		if numberOfMembers == 0 {
			// if group is empty, continue.
			break
		}
		// otherwise wait a bit and double wait time.
		time.Sleep(waitTime)
		waitTime *= 2
	}
	// check if group is finally empty.
	require.Equal(t, 0, numberOfMembers, "Consumer group is not empty after checking %d time.", maxTries)

	// Open a new client and pull the second batch again.
	newClient, err := kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}, batchConsumerSettings)
	require.NoError(t, err, "BatchIterator could not be created.")

	defer newClient.Close()

	// Pull second batch again.
	ctxWithTimeout, cancel = context.WithTimeout(context.Background(), TimeoutDuration)

	defer cancel()

	batch2SecondPull, err := newClient.NextBatch(ctxWithTimeout)
	require.NoError(t, err, "Could not pull the second batch again.")
	// Compare contents of the two batches.
	require.Equal(
		t,
		len(batch2),
		len(batch2SecondPull),
		"Length of the second pulled batch is not equal.",
	)

	for i, record := range batch2SecondPull { //nolint:varnamelen //intentionally short
		require.EqualValues(
			t,
			batch2[i].Value,
			record.Value,
			"Payloads are not equal. Expected %v received %v",
			batch2[i].Value,
			record.Value,
		)
	}

	// commit and check offset is equal to number of all sent messages.
	brokerutil.CommitHighestOffsets(batch2SecondPull) // batch2SecondPull.Ack().
	checkNumberOfCommittedAllPartitions(ctx, t, topicName, groupID, int32(partitionNum), int64(len(sentRecords)))
}

func getNumberOfMembers(ctx context.Context, t *testing.T, groupID string) int {
	t.Helper()

	kgoClient, err := kgo.NewClient(kgo.SeedBrokers(BrokerAddrs)) //nolint:contextcheck //cannot pass the context
	require.NoError(t, err, "Could not create franz go client.")

	adminClient := kadm.NewClient(kgoClient)

	defer adminClient.Close() //nolint:contextcheck //cannot pass the context

	ctxWithTimeout, cancel := context.WithTimeout(ctx, TimeoutDuration)
	defer cancel()

	describedGroups, err := adminClient.DescribeGroups(ctxWithTimeout, groupID)
	require.NoError(t, err, "Could not get information about consumer group.")

	return len(describedGroups[groupID].Members)
}

func publishMessages(ctx context.Context, t *testing.T, records ...*kgo.Record) {
	t.Helper()

	kgoClient, err := kgo.NewClient( //nolint:contextcheck //cannot pass the context
		kgo.SeedBrokers(BrokerAddrs),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)
	require.NoError(t, err, "Could not create franz-go client.")

	defer kgoClient.Close() //nolint:contextcheck //cannot pass the context

	ctxWithTimeout, cancel := context.WithTimeout(ctx, TimeoutDuration)
	defer cancel()

	err = kgoClient.ProduceSync(ctxWithTimeout, records...).FirstErr()
	require.NoError(t, err, "Could not publish message.")
}

func checkNumberOfCommitted(ctx context.Context, t *testing.T, topicName, groupID string, partition int32, expected int64) {
	t.Helper()

	client, err := kgo.NewClient( //nolint:contextcheck //cannot pass the context
		kgo.SeedBrokers(BrokerAddrs),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topicName),
	)
	require.NoError(t, err, "Could not create franz go client.")

	defer client.Close() //nolint:contextcheck //cannot pass the context

	adminClient := kadm.NewClient(client)

	offsetsForTopic, err := adminClient.FetchOffsetsForTopics(ctx, groupID, topicName)
	require.NoError(t, err, "Offsets for topic %s could not be fetched.", topicName)
	actual := offsetsForTopic[topicName][partition].Offset.At
	require.Equal(t, expected, actual, "Expected and actual offsets are not equal.")
}

func checkNumberOfCommittedAllPartitions(ctx context.Context, t *testing.T, topicName, groupID string, partitions int32, expected int64) {
	t.Helper()

	client, err := kgo.NewClient( //nolint:contextcheck //cannot pass the context
		kgo.SeedBrokers(BrokerAddrs),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topicName),
	)
	require.NoError(t, err, "Could not create franz go client.")

	defer client.Close() //nolint:contextcheck //cannot pass the context

	adminClient := kadm.NewClient(client)

	offsetsForTopic, err := adminClient.FetchOffsetsForTopics(ctx, groupID, topicName)
	require.NoError(t, err, "Offsets for topic %s could not be fetched.", topicName)

	var actual int64

	for i := int32(0); i < partitions; i++ {
		if offsetsForTopic[topicName][i].Offset.At == -1 {
			continue
		}

		actual += offsetsForTopic[topicName][i].Offset.At
	}
	require.Equal(t, expected, actual, "Expected and actual offsets are not equal.")
}

func assertEqualAsSent(t *testing.T, sent kgo.Record, received broker.Record) {
	t.Helper()

	require.EqualValues(
		t,
		sent.Key,
		received.Key,
		"Keys are not equal. Expected: %v received: %v",
		sent.Key,
		received.Key,
	)
	require.EqualValues(
		t,
		sent.Value,
		received.Value,
		"Payloads are not equal. Expected: %v received: %v",
		string(sent.Value),
		string(received.Value),
	)

	for _, header := range sent.Headers {
		receivedAttribute := received.Attributes[header.Key]
		require.EqualValues(
			t,
			header.Value,
			receivedAttribute,
			"Attributes with key %v are not equal. Expected: %v received: %v",
			header.Key,
			header.Value,
			receivedAttribute,
		)
	}
}

// TODO figure out how to use generics to use AssertEqualAsSent and not a copy of it?
func assertEqualAsSent2(t *testing.T, sent kgo.Record, received broker.Record) {
	t.Helper()

	require.EqualValues(
		t,
		sent.Key,
		received.Key,
		"Keys are not equal. Expected: %v received: %v",
		sent.Key,
		received.Key,
	)
	require.EqualValues(
		t,
		sent.Value,
		received.Value,
		"Payloads are not equal. Expected: %v received: %v",
		string(sent.Value),
		string(received.Value),
	)

	for _, header := range sent.Headers {
		receivedAttribute := received.Attributes[header.Key]
		require.EqualValues(
			t,
			header.Value,
			receivedAttribute,
			"Attributes with key %v are not equal. Expected: %v received: %v",
			header.Key,
			header.Value,
			receivedAttribute,
		)
	}
}
