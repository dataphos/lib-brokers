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
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dataphos/lib-brokers/internal/errtemplates"
	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/servicebus"
)

func TestNewBatchIterator(t *testing.T) {
	if connectionString == "" {
		t.Skip("Connection string not set, skipping integration test")
	}

	t.Run("successfully create a BatchIterator", func(t *testing.T) {
		config := servicebus.IteratorConfig{
			ConnectionString: connectionString,
			Topic:            topicID,
			Subscription:     subscriptionID,
		}
		settings := servicebus.DefaultBatchIteratorSettings

		batchIterator, err := servicebus.NewBatchIterator(config, settings)
		require.NoError(t, err)
		require.NotNil(t, batchIterator)

		defer func() {
			err := batchIterator.Close()
			assert.NoError(t, err)
		}()
	})

	t.Run("fail to create a BatchIterator with oversized batch size", func(t *testing.T) {
		config := servicebus.IteratorConfig{
			ConnectionString: connectionString,
			Topic:            topicID,
			Subscription:     subscriptionID,
		}
		settings := servicebus.BatchIteratorSettings{
			BatchSize: 6000,
		}

		batchIterator, err := servicebus.NewBatchIterator(config, settings)
		assert.Error(t, err)
		assert.Nil(t, batchIterator)
		require.Equal(t, errtemplates.BatchSizeTooBig(servicebus.MaxIteratorBatchSize, settings.BatchSize).Error(), err.Error())
	})
}

func TestNextBatch(t *testing.T) {
	if connectionString == "" {
		t.Skip("Connection string not set, skipping integration test")
	}

	// Send a message to the Service Bus for the test.
	messages := []broker.OutboundMessage{
		{Data: []byte("message 1"), Attributes: map[string]interface{}{"key1": "value1"}},
		{Data: []byte("message 2"), Attributes: map[string]interface{}{"key2": "value2"}},
		{Data: []byte("message 3"), Attributes: map[string]interface{}{"key3": "value3"}},
	}

	err := SendMessages(connectionString, topicID, messages)
	assert.NoError(t, err)

	config := servicebus.IteratorConfig{
		ConnectionString: connectionString,
		Topic:            topicID,
		Subscription:     subscriptionID,
	}
	settings := servicebus.DefaultBatchIteratorSettings

	batchIterator, err := servicebus.NewBatchIterator(config, settings)
	assert.NoError(t, err)
	assert.NotNil(t, batchIterator)

	defer func() {
		err := batchIterator.Close()
		assert.NoError(t, err)
	}()

	t.Run("successfully fetch a batch of messages", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		results, err := batchIterator.NextBatch(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, messages)
		assert.Greater(t, len(messages), 0)

		var received bool
		for _, message := range messages {
			received = false
			for _, result := range results {
				if reflect.DeepEqual(message.Data, result.Data) {
					received = true
				}
			}
			if !received {
				t.Fatalf("Message [%s] not received", string(message.Data))
			}
		}
	})
}

func SendMessages(connectionString, topic string, messages []broker.OutboundMessage) error {
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err := client.Close(context.Background()); err != nil {
			log.Printf("Error closing Service Bus client: %v", err)
		}
	}()

	sender, err := client.NewSender(topic, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err := sender.Close(context.Background()); err != nil {
			log.Printf("Error closing Service Bus sender: %v", err)
		}
	}()

	for _, msg := range messages {
		azMsg := &azservicebus.Message{
			ApplicationProperties: msg.Attributes,
			Body:                  msg.Data,
		}
		if err := sender.SendMessage(context.Background(), azMsg, nil); err != nil {
			return err
		}
	}

	return nil
}
