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

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

type Publisher struct {
	Client *azservicebus.Client
}

func NewPublisher(connectionString string) (*Publisher, error) {
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}

	return &Publisher{
		Client: client,
	}, nil
}

func (p *Publisher) Topic(topicID string) (broker.Topic, error) {
	sender, err := p.Client.NewSender(topicID, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "creating topic %s failed", topicID)
	}

	return &Topic{
		sender: sender,
	}, nil
}

func (p *Publisher) Close() error {
	return p.Client.Close(context.Background())
}

type Topic struct {
	sender *azservicebus.Sender
}

func (t *Topic) Publish(ctx context.Context, message broker.OutboundMessage) error {
	return t.sender.SendMessage(ctx, intoServiceBusMessage(message), nil)
}

func (t *Topic) BatchPublish(ctx context.Context, messages ...broker.OutboundMessage) error {
	batch, err := t.sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return err
	}
	// add messages.
	for _, msg := range messages {
		if addError := batch.AddMessage(intoServiceBusMessage(msg), nil); addError != nil {
			if batch.NumMessages() <= 0 || !errors.Is(addError, azservicebus.ErrMessageTooLarge) {
				return addError
			}

			// too many messages for one batch, send it and make a new one.
			batchToSend := batch
			if err = t.sender.SendMessageBatch(ctx, batchToSend, nil); err != nil {
				return err
			}

			batch, err = t.sender.NewMessageBatch(ctx, nil)
			if err != nil {
				return err
			}

			if addError = batch.AddMessage(intoServiceBusMessage(msg), nil); addError != nil {
				// batch is already empty so this message is too big.
				return addError
			}
		}
	}

	return t.sender.SendMessageBatch(ctx, batch, nil)
}

func intoServiceBusMessage(message broker.OutboundMessage) *azservicebus.Message {
	var key *string
	if message.Key != "" {
		key = &message.Key
	}

	return &azservicebus.Message{
		Body:                  message.Data,
		PartitionKey:          key,
		ApplicationProperties: message.Attributes,
	}
}
