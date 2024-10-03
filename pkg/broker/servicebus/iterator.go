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

package servicebus

import (
	"context"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/internal/errtemplates"
	"github.com/dataphos/lib-brokers/pkg/broker"
)

const MaxIteratorBatchSize = 5000

type BatchIterator struct {
	receiver *azservicebus.Receiver
	Settings BatchIteratorSettings
}

type IteratorConfig struct {
	ConnectionString string
	Topic            string
	Subscription     string
}

type BatchIteratorSettings struct {
	BatchSize int
}

var DefaultBatchIteratorSettings = BatchIteratorSettings{
	BatchSize: 100,
}

func NewBatchIterator(config IteratorConfig, settings BatchIteratorSettings) (*BatchIterator, error) {
	if settings.BatchSize > MaxIteratorBatchSize {
		return nil, errtemplates.BatchSizeTooBig(MaxIteratorBatchSize, settings.BatchSize)
	}

	client, err := azservicebus.NewClientFromConnectionString(config.ConnectionString, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}

	receiver, err := client.NewReceiverForSubscription(
		config.Topic,
		config.Subscription,
		&azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create receiver")
	}

	return &BatchIterator{
		receiver: receiver,
		Settings: settings,
	}, nil
}

func (it *BatchIterator) NextBatch(ctx context.Context) ([]broker.Message, error) {
	received, err := it.receiver.ReceiveMessages(ctx, it.Settings.BatchSize, nil)
	if err != nil {
		return nil, err
	}

	messages := make([]broker.Message, len(received))
	for i := range received {
		messages[i] = it.intoBrokerMessage(ctx, received[i])
	}

	return messages, nil
}

func (it *BatchIterator) intoBrokerMessage(_ context.Context, message *azservicebus.ReceivedMessage) broker.Message {
	var key string
	if message.PartitionKey != nil {
		key = *message.PartitionKey
	}

	return broker.Message{
		ID:         message.MessageID,
		Key:        key,
		Data:       message.Body,
		Attributes: message.ApplicationProperties,
		AckFunc: func() {
			_ = it.receiver.CompleteMessage(context.Background(), message, nil)
		},
		NackFunc: func() {
			_ = it.receiver.AbandonMessage(context.Background(), message, nil)
		},
		// dereferencing could panic, so this needs to be analyzed further.
		PublishTime:   *message.EnqueuedTime,
		IngestionTime: time.Now(),
	}
}

func (it *BatchIterator) Close() error {
	return it.receiver.Close(context.Background())
}
