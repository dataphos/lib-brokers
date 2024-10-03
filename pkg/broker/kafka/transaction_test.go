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

	"github.com/stretchr/testify/require"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
)

func TestNewTransaction(t *testing.T) {
	topicName := "test.new.transaction"
	groupID := topicName + ".group"
	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}
	// Test transaction creation success.
	transaction, err := kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	require.NoError(t, err, "Could not create transaction.")
	transaction.Close()

	differentBrokerAddress := "localHost:9090"
	consumerConfig.BrokerAddr = differentBrokerAddress
	// Test transaction creation fail.
	transaction, err = kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	if err == nil {
		if transaction != nil {
			transaction.Close()
		}

		t.Fatal("An error was expected as the broker addresses do not match.")
	}
}

func TestTransaction_Begin(t *testing.T) {
	topicName := "test.transaction.begin"
	groupID := topicName + ".group"
	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}

	require.Equalf(t, "", producerConfig.TransactionalID, "publisher ID should be an empty string, got %s", producerConfig.TransactionalID)

	// Test transaction creation success.
	transaction, err := kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	require.NoError(t, err, "Could not create transaction.")

	defer transaction.Close()
	err = transaction.Begin()
	require.NotNilf(t, err, "expected to get the error about the producer being non-transactional")
}

func TestTransaction_Topic(t *testing.T) {
	topicName := "test.transaction.topic"
	groupID := topicName + ".group"
	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}

	transaction, err := kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	require.NoError(t, err, "Could not create transaction.")
	transaction.Close()

	topicID := "transactional.topic"
	topic, err := transaction.Topic(topicID)
	require.NoError(t, err, "could not create a transactional topic")
	require.NotNil(t, topic, "topic should not be nil")
}

func TestTransaction_Abort(t *testing.T) {
	topicName := "test.transaction.abort"
	groupID := topicName + ".group"
	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}
	producerConfig := kafka.ProducerConfig{
		BrokerAddr:      BrokerAddrs,
		TransactionalID: "12345",
	}

	transaction, err := kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	require.NoError(t, err, "Could not create transaction.")

	defer transaction.Close()

	err = transaction.Begin()
	require.Nilf(t, err, "expected to get no error")
	t.Log("transaction begin")

	committed, err := transaction.Abort(context.Background())
	require.False(t, committed)
	require.Nilf(t, err, "expected to get no error")
	t.Log("aborted the transaction")
}

func TestTransaction_Commit(t *testing.T) {
	topicName := "test.transaction.commit"
	groupID := topicName + ".group"
	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}

	producerConfig := kafka.ProducerConfig{
		BrokerAddr:      BrokerAddrs,
		TransactionalID: "12345",
	}

	transaction, err := kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	require.NoError(t, err, "Could not create transaction.")

	defer transaction.Close()

	err = transaction.Begin()
	require.Nilf(t, err, "expected to get no error")
	t.Log("transaction begin")

	committed, err := transaction.Commit(context.Background())
	require.Nilf(t, err, "expected to get no error")
	require.True(t, committed)
	t.Log("committed the transaction")
}

func TestTransactionalTopic_Publish(t *testing.T) {
	topicName := "test.transaction.topic.publish"
	groupID := topicName + ".group"
	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}

	producerConfig := kafka.ProducerConfig{
		BrokerAddr:      BrokerAddrs,
		TransactionalID: "12345",
	}

	transaction, err := kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	require.NoError(t, err, "Could not create transaction.")

	defer transaction.Close()

	topicID := "transactional.topic"
	topic, err := transaction.Topic(topicID)
	require.NoError(t, err, "could not create a transactional topic")
	require.NotNil(t, topic, "topic should not be nil")

	err = transaction.Begin()
	require.Nilf(t, err, "expected to get no error")
	t.Log("transaction begin")

	message := broker.OutboundMessage{
		Key:        "key",
		Data:       []byte("A simple message."),
		Attributes: map[string]interface{}{"customKey": "customValue"},
	}
	for i := 0; i < 2; i++ {
		message.Key = fmt.Sprintf("key%d", i)
		err = topic.Publish(context.Background(), message)
		require.Nilf(t, err, "expected to get no error")
		t.Logf("published a message to topic %s", topicID)
	}

	transactionalConsumerSettings := batchConsumerSettings
	transactionalConsumerSettings.Transactional = true
	transactionalIteratorClient, err := kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicID,
	}, transactionalConsumerSettings)
	require.NoError(t, err, "BatchIterator could not be created.")

	defer transactionalIteratorClient.Close()

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	batch1, err := transactionalIteratorClient.NextBatch(ctxWithTimeout)

	if err == nil {
		if len(batch1) > 0 {
			for _, rec := range batch1 {
				t.Log(rec.Key + ": " + string(rec.Value))
			}
		}

		require.Equalf(t, 0, len(batch1), "expected to pull 0 messages, since the transaction hasn't been committed")
	} else {
		t.Fatal(err)
	}

	committed, err := transaction.Commit(context.Background())
	require.True(t, committed)
	require.Nilf(t, err, "expected to get no error")
	t.Log("committed the transaction")

	ctxWithTimeout, cancel = context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	batch2, err := transactionalIteratorClient.NextBatch(ctxWithTimeout)
	require.NoError(t, err, "Could not pull first batch.")
	require.Greater(t, len(batch2), 0, "expected to pull more than 0 messages.")

	for _, record := range batch2 {
		t.Log(record)
	}

	require.Equalf(t, 2, len(batch2), "expected to receive 2 messages")
	transaction.Close()
}

func TestTransaction_NextBatch_Uncommitted(t *testing.T) {
	topicName := "test.transaction.topic.uncommitted"
	groupID := topicName + ".group"
	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicName,
	}

	producerConfig := kafka.ProducerConfig{
		BrokerAddr:      BrokerAddrs,
		TransactionalID: "12345",
	}

	transaction, err := kafka.NewTransaction(context.Background(), producerConfig, producerSettings, consumerConfig, batchConsumerSettings)
	require.NoError(t, err, "Could not create transaction.")

	defer transaction.Close()

	topicID := "transactional.topic"
	topic, err := transaction.Topic(topicID)
	require.NoError(t, err, "could not create a transactional topic")
	require.NotNil(t, topic, "topic should not be nil")

	err = transaction.Begin()
	require.Nilf(t, err, "expected to get no error")
	t.Log("transaction begin")

	message := broker.OutboundMessage{
		Key:        "key",
		Data:       []byte("A simple message."),
		Attributes: map[string]interface{}{"customKey": "customValue"},
	}
	for i := 0; i < 2; i++ {
		message.Key = fmt.Sprintf("key%d", i)
		err = topic.Publish(context.Background(), message)
		require.Nilf(t, err, "expected to get no error")
		t.Logf("published a message to topic %s", topicID)
	}

	nonTransactionalIteratorClient, err := kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicID,
	}, batchConsumerSettings)
	require.NoError(t, err, "BatchIterator could not be created.")

	defer nonTransactionalIteratorClient.Close()

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	batch1, err := nonTransactionalIteratorClient.NextBatch(ctxWithTimeout)

	if err == nil {
		require.GreaterOrEqualf(t, len(batch1), 2, "expected to pull >=2 messages, even though the transaction hasn't been committed")

		for _, record := range batch1 {
			t.Log(record)
		}
	} else {
		t.Fatal(err)
	}

	committed, err := transaction.Commit(context.Background())
	require.True(t, committed)
	require.Nilf(t, err, "expected to get no error")
	t.Log("committed the transaction")

	transaction.Close()

	transactionalConsumerSettings := batchConsumerSettings
	transactionalConsumerSettings.Transactional = true
	transactionalIteratorClient, _ := kafka.NewBatchIterator(context.Background(), kafka.ConsumerConfig{
		BrokerAddr: BrokerAddrs,
		GroupID:    groupID,
		Topic:      topicID,
	}, transactionalConsumerSettings)
	ctxWithTimeout, cancel = context.WithTimeout(context.Background(), TimeoutDuration)

	defer cancel()

	batch2, err := transactionalIteratorClient.NextBatch(ctxWithTimeout)

	if err == nil {
		require.Equalf(t, 0, len(batch2), "expected to pull 0 messages")
	} else {
		t.Fatal(err)
	}
}
