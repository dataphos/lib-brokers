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
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// Publisher models a Pulsar producer.
type Publisher struct {
	client pulsar.Client

	// producerOptions configuration of producer specifics.
	producerOptions *pulsar.ProducerOptions
}

// PublisherConfig defines the configuration properties required for initializing a Pulsar producer.
type PublisherConfig struct {
	// ServiceURL the URL of the Pulsar service.
	ServiceURL string

	// TLSConfig tls configuration for the Pulsar client.
	//
	// If nil, the publisher will not use tls.
	TLSConfig *tls.Config
}

// PublisherSettings the optional settings for a Pulsar producer.
type PublisherSettings struct {
	// ConnectionTimeout timeout for the establishment of a TCP connection.
	ConnectionTimeout time.Duration

	// OperationTimeout timeout for creating the publisher.
	//
	// After this duration, it will return an error.
	OperationTimeout time.Duration

	// SendTimeout timeout for a published message to be acknowledged by the broker.
	//
	// After timing out, error is returned.
	// If set to negative value, such as -1, the timeout is disabled.
	SendTimeout time.Duration

	// MaxConnectionsPerBroker max number of connections to a single broker that will be kept in the pool.
	MaxConnectionsPerBroker int

	// DisableBlockIfQueueFull controls whether publishing blocks if producer's message queue is full.
	// Default is false, if set to true then Publish returns error when queue is full.
	DisableBlockIfQueueFull bool

	// MaxPendingMessages specifies the max size of the queue holding messages waiting an acknowledgment from the broker.
	MaxPendingMessages int

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

// DefaultPublisherSettings stores the default values for ProducerSettings.
var DefaultPublisherSettings = PublisherSettings{
	ConnectionTimeout:          5 * time.Second,
	OperationTimeout:           30 * time.Second,
	SendTimeout:                30 * time.Second,
	MaxConnectionsPerBroker:    1,
	DisableBlockIfQueueFull:    false,
	MaxPendingMessages:         1,
	MaxReconnectToBroker:       nil,
	TLSTrustCertsFilePath:      "",
	TLSAllowInsecureConnection: false,
}

func NewPublisher(config PublisherConfig, settings PublisherSettings) (*Publisher, error) {
	client, producerOptions, err := configurePublisher(config, settings)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client:          client,
		producerOptions: producerOptions,
	}, nil
}

// configurePublisher configures the github.com/apache/pulsar-client-go/pulsar client and PublisherOptions.
func configurePublisher( //nolint:ireturn //have to return interface
	config PublisherConfig,
	settings PublisherSettings,
) (pulsar.Client, *pulsar.ProducerOptions, error) {
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
			return nil, nil, err
		}
	}

	client, err := pulsar.NewClient(options)
	if err != nil {
		return nil, nil, err
	}

	producerOptions := pulsar.ProducerOptions{
		SendTimeout:             settings.SendTimeout,
		DisableBlockIfQueueFull: settings.DisableBlockIfQueueFull,
		MaxPendingMessages:      settings.MaxPendingMessages,
	}

	return client, &producerOptions, nil
}

func (p *Publisher) Topic(topicID string) (broker.Topic, error) {
	options := p.producerOptions
	options.Topic = topicID
	producer, err := p.client.CreateProducer(*options)
	if err != nil { //nolint:wsl //file is gofumt-ed
		return &Topic{
			producer: nil,
			topicID:  "",
		}, err
	}

	return &Topic{
		producer: producer,
		topicID:  topicID,
	}, nil
}

func (p *Publisher) Close() error {
	p.client.Close()

	return nil
}

type Topic struct {
	producer pulsar.Producer
	topicID  string
}

func (t *Topic) Publish(ctx context.Context, message broker.OutboundMessage) error {
	if _, err := t.producer.Send(ctx, intoPulsarMessage(message)); err != nil {
		return err
	}

	return nil
}

func (t *Topic) BatchPublish(ctx context.Context, messages ...broker.OutboundMessage) error {
	return broker.SimplePublisherParallelization(ctx, t, messages...)
}

func intoPulsarMessage(message broker.OutboundMessage) *pulsar.ProducerMessage {
	var properties map[string]string
	if message.Attributes != nil {
		properties = make(map[string]string, len(message.Attributes))

		for key, value := range message.Attributes {
			if value == nil {
				panic(fmt.Sprintf("Value of attribute %q is nil. An attribute's value should not be nil.", key))
			}

			if v, ok := value.(string); ok {
				properties[key] = v

				continue
			}

			properties[key] = fmt.Sprintf("%s", value)
		}
	}

	return &pulsar.ProducerMessage{
		Payload:    message.Data,
		Key:        message.Key,
		Properties: properties,
	}
}

func (t *Topic) Close() error {
	if err := t.producer.Flush(); err != nil {
		return err
	}

	t.producer.Close()

	return nil
}
