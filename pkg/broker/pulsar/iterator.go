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

package pulsar

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

type Iterator struct {
	consumer pulsar.Consumer
}

type IteratorConfig struct {
	// ServiceURL the URL of the Pulsar service.
	ServiceURL string

	// Topic name of the topic.
	Topic string

	// Subscription name of the subscription.
	Subscription string

	// TLSConfig tls configuration for the Pulsar client.
	//
	// If nil, the iterator will not use tls.
	TLSConfig *tls.Config
}

type IteratorSettings struct {
	// ConnectionTimeout timeout for the establishment of a TCP connection.
	ConnectionTimeout time.Duration

	// OperationTimeout timeout for creating the iterator.
	//
	// After this duration, it will return an error.
	OperationTimeout time.Duration

	// NackRedeliveryDelay delay after which to redeliver the messages that failed to be processed.
	NackRedeliveryDelay time.Duration

	// MaxConnectionsPerBroker max number of connections to a single broker that will be kept in the pool.
	MaxConnectionsPerBroker int

	// SubscriptionType determines the mode in which messages are dispatched and consumed for this subscription.
	SubscriptionType SubscriptionType

	// ReceiverQueueSize sets the size of the consumer receive queue.
	//
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application calls `Consumer.receive()`. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	ReceiverQueueSize int

	// MaxReconnectToBroker specifies the maximum retry number of reconnectToBroker.
	//
	// If nil, the client will retry forever.
	MaxReconnectToBroker *uint

	// TLSTrustCertsFilePath path to trusted certificate file.
	//
	// If this is not set, TLSAllowInsecureConnection should be set to `true`.
	TLSTrustCertsFilePath string

	// TLSAllowInsecureConnection controls if the client accepts untrusted TLS certificate from the broker.
	//
	// If this is set to `true`, TLSTrustCertsFilePath is not needed.
	TLSAllowInsecureConnection bool
}

type SubscriptionType int

const (
	// Exclusive there can be only 1 consumer on the same topic with the same subscription name.
	Exclusive SubscriptionType = iota

	// Shared subscription mode.
	//
	// Multiple consumer are able to use the same subscription name and messages are dispatched according to
	// a round-robin rotation between the connected consumers.
	Shared

	// Failover subscription mode.
	//
	// Multiple consumer are able to use the same subscription name but only 1 consumer receives the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover

	// KeyShared subscription mode.
	//
	// Multiple consumer are able to use the same subscription name
	// and all messages with the same key are dispatched to the same consumer.
	KeyShared
)

var DefaultIteratorSettings = IteratorSettings{
	ConnectionTimeout:          5 * time.Second,
	OperationTimeout:           30 * time.Second,
	NackRedeliveryDelay:        30 * time.Second,
	MaxConnectionsPerBroker:    1,
	SubscriptionType:           Exclusive,
	ReceiverQueueSize:          1000,
	MaxReconnectToBroker:       nil,
	TLSTrustCertsFilePath:      "",
	TLSAllowInsecureConnection: false,
}

func NewIterator(config IteratorConfig, settings IteratorSettings) (*Iterator, error) {
	consumer, err := configureConsumer(config, settings)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		consumer: consumer,
	}, nil
}

func configureConsumer(config IteratorConfig, settings IteratorSettings) (pulsar.Consumer, error) { //nolint:ireturn //have to return interface
	options := pulsar.ClientOptions{
		URL:                     config.ServiceURL,
		ConnectionTimeout:       settings.ConnectionTimeout,
		OperationTimeout:        settings.OperationTimeout,
		MaxConnectionsPerBroker: settings.MaxConnectionsPerBroker,
	}

	if config.TLSConfig != nil {
		var err error

		options, err = configureTLS(config.TLSConfig, settings.TLSTrustCertsFilePath, settings.TLSAllowInsecureConnection, options)
		if err != nil {
			return nil, err
		}
	}

	client, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}

	// Map the subscription type to pulsar's subscription type.
	var subscriptionType pulsar.SubscriptionType

	switch settings.SubscriptionType {
	case Exclusive:
		subscriptionType = pulsar.KeyShared
	case Shared:
		subscriptionType = pulsar.Shared
	case Failover:
		subscriptionType = pulsar.Failover
	case KeyShared:
		subscriptionType = pulsar.KeyShared
	}

	consumerOptions := pulsar.ConsumerOptions{
		Topic:                config.Topic,
		SubscriptionName:     config.Subscription,
		Type:                 subscriptionType,
		ReceiverQueueSize:    settings.ReceiverQueueSize,
		NackRedeliveryDelay:  settings.NackRedeliveryDelay,
		MaxReconnectToBroker: settings.MaxReconnectToBroker,
	}

	return client.Subscribe(consumerOptions)
}

func (it *Iterator) NextMessage(ctx context.Context) (broker.Message, error) {
	message, err := it.consumer.Receive(ctx)
	if err != nil {
		return broker.Message{}, err
	}

	return it.intoBrokerMessage(message), nil
}

func (it *Iterator) intoBrokerMessage(message pulsar.Message) broker.Message {
	var attributes map[string]interface{}

	if message.Properties() != nil {
		properties := message.Properties()
		attributes = make(map[string]interface{}, len(properties))

		for k, v := range properties {
			attributes[k] = v
		}
	}

	return broker.Message{
		ID:            string(message.ID().Serialize()),
		Key:           message.Key(),
		Data:          message.Payload(),
		Attributes:    attributes,
		PublishTime:   message.PublishTime(),
		IngestionTime: time.Now(),
		AckFunc: func() {
			_ = it.consumer.Ack(message)
		},
		NackFunc: func() {
			it.consumer.Nack(message)
		},
	}
}

func (it *Iterator) Close() error {
	if err := it.consumer.Unsubscribe(); err != nil {
		return err
	}

	it.consumer.Close()

	return nil
}
