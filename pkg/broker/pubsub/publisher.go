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

package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

type Publisher struct {
	client                *pubsub.Client
	Settings              pubsub.PublishSettings
	enableMessageOrdering bool
	skipTopicHealthCheck  bool
}

// PublisherConfig defines configuration properties needed for initializing a Pub/Sub producer.
type PublisherConfig struct {
	// ProjectID is the unique identifier of the GCP project.
	ProjectID string

	// GrpcClientConn optionally sets the grpc.ClientConn which is used for communicating with Pub/Sub.
	// See GitHub issue on this link for more information:
	// https://github.com/googleapis/google-cloud-go/issues/1410
	GrpcClientConn *grpc.ClientConn
}

type PublishSettings struct {
	DelayThreshold         time.Duration
	CountThreshold         int
	ByteThreshold          int
	NumGoroutines          int
	Timeout                time.Duration
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	EnableMessageOrdering  bool
}

var DefaultPublishSettings = PublishSettings{
	DelayThreshold:         50 * time.Millisecond,
	CountThreshold:         50,
	ByteThreshold:          50 * 1024 * 1024,
	NumGoroutines:          5,
	Timeout:                15 * time.Second,
	MaxOutstandingMessages: 800,
	MaxOutstandingBytes:    1000 * 1024 * 1024,
	EnableMessageOrdering:  false,
}

func NewPublisher(ctx context.Context, config PublisherConfig, settings PublishSettings) (*Publisher, error) {
	var opts []option.ClientOption

	skipTopicHealthCheck := false

	if config.GrpcClientConn != nil {
		opts = append(opts, option.WithGRPCConn(config.GrpcClientConn))
		skipTopicHealthCheck = true
	}

	client, err := pubsub.NewClient(ctx, config.ProjectID, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create pubsub client")
	}

	return &Publisher{
		client:                client,
		Settings:              intoPubsubPublishSettings(settings),
		enableMessageOrdering: settings.EnableMessageOrdering,
		skipTopicHealthCheck:  skipTopicHealthCheck,
	}, nil
}

func intoPubsubPublishSettings(settings PublishSettings) pubsub.PublishSettings {
	return pubsub.PublishSettings{
		DelayThreshold: settings.DelayThreshold,
		CountThreshold: settings.CountThreshold,
		ByteThreshold:  settings.ByteThreshold,
		NumGoroutines:  settings.NumGoroutines,
		Timeout:        settings.Timeout,
		FlowControlSettings: pubsub.FlowControlSettings{
			MaxOutstandingMessages: settings.MaxOutstandingMessages,
			MaxOutstandingBytes:    settings.MaxOutstandingBytes,
			LimitExceededBehavior:  pubsub.FlowControlBlock,
		},
	}
}

func (p *Publisher) Topic(topicID string) (broker.Topic, error) {
	topic := p.client.Topic(topicID)
	topic.PublishSettings = p.Settings
	topic.EnableMessageOrdering = p.enableMessageOrdering

	if !p.skipTopicHealthCheck {
		if err := doTopicHealthCheck(context.Background(), topic); err != nil {
			return &Topic{
				topic: nil,
			}, err
		}
	}

	return &Topic{
		topic: topic,
	}, nil
}

func (p *Publisher) Close() error {
	return p.client.Close()
}

// doTopicHealthCheck checks if the topic resource exists and then checks if the service account linked to it
// has sufficient permissions for publishing messages.
func doTopicHealthCheck(ctx context.Context, topic *pubsub.Topic) error {
	exists, err := topic.Exists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return errors.Errorf("given topic (%s) does not exist", topic.ID())
	}

	return testPermissions(ctx, topic.IAM(), []string{"pubsub.topics.publish"})
}

type Topic struct {
	topic *pubsub.Topic
}

func (t *Topic) Publish(ctx context.Context, message broker.OutboundMessage) error {
	result := t.topic.Publish(ctx, t.intoPubsubMessage(message))
	_, err := result.Get(ctx)
	if err != nil { //nolint:wsl //file gofumpt-ed
		return err
	}

	return nil
}

func (t *Topic) BatchPublish(ctx context.Context, messages ...broker.OutboundMessage) error {
	return broker.SimplePublisherParallelization(ctx, t, messages...)
}

func (t *Topic) intoPubsubMessage(message broker.OutboundMessage) *pubsub.Message {
	var attributes map[string]string
	if message.Attributes != nil {
		attributes = make(map[string]string, len(message.Attributes))

		for key, value := range message.Attributes {
			if v, ok := value.(string); ok {
				attributes[key] = v

				continue
			}

			attributes[key] = fmt.Sprintf("%s", value)
		}
	}

	// if the message ordering feature is not set, but the OutboundMessage contains a key,
	// Pub/Sub will return an error when publishing, so we change this behavior by ignoring the OutboundMessage.Key instead.
	var orderingKey string
	if t.topic.EnableMessageOrdering {
		orderingKey = message.Key
	}

	return &pubsub.Message{
		Data:        message.Data,
		OrderingKey: orderingKey,
		Attributes:  attributes,
	}
}
