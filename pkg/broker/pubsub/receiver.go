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
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

type Receiver struct {
	subscription *pubsub.Subscription
	client       *pubsub.Client
}

// ReceiverConfig defines configuration properties needed for initializing a Pub/Sub receiver.
type ReceiverConfig struct {
	// ProjectID is the unique identifier of the GCP project.
	ProjectID string

	// SubscriptionID is the unique identifier of the Pub/Sub subscription.
	SubscriptionID string

	// GrpcClientConn optionally sets the grpc.ClientConn which is used for communicating with Pub/Sub.
	// See GitHub issue on this link for more information:
	// https://github.com/googleapis/google-cloud-go/issues/1410
	GrpcClientConn *grpc.ClientConn
}

type ReceiveSettings struct {
	MaxExtension           time.Duration
	MaxExtensionPeriod     time.Duration
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	NumGoroutines          int
}

var DefaultReceiveSettings = ReceiveSettings{
	MaxExtension:           30 * time.Minute,
	MaxExtensionPeriod:     3 * time.Minute,
	MaxOutstandingMessages: 1000,
	MaxOutstandingBytes:    400 * 1024 * 1024,
	NumGoroutines:          10,
}

func NewReceiver(ctx context.Context, config ReceiverConfig, settings ReceiveSettings) (*Receiver, error) {
	var opts []option.ClientOption
	if config.GrpcClientConn != nil {
		opts = append(opts, option.WithGRPCConn(config.GrpcClientConn))
	}

	client, err := pubsub.NewClient(ctx, config.ProjectID, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create pubsub client")
	}

	subscription := client.Subscription(config.SubscriptionID)
	subscription.ReceiveSettings = intoPubsubReceiveSettings(settings)

	// pstest does not support IAM policy check, so this step is skipped if grpc.ClientConn is used.
	if config.GrpcClientConn == nil {
		if err = doSubscriptionHealthCheck(ctx, subscription); err != nil {
			return nil, err
		}
	}

	return &Receiver{
		subscription: subscription,
		client:       client,
	}, nil
}

func intoPubsubReceiveSettings(settings ReceiveSettings) pubsub.ReceiveSettings {
	return pubsub.ReceiveSettings{
		MaxExtension:           settings.MaxExtension,
		MaxExtensionPeriod:     settings.MaxExtensionPeriod,
		MaxOutstandingMessages: settings.MaxOutstandingMessages,
		MaxOutstandingBytes:    settings.MaxOutstandingBytes,
		UseLegacyFlowControl:   false,
		NumGoroutines:          settings.NumGoroutines,
	}
}

// doSubscriptionHealthCheck checks if the subscription resource exists and then checks if the service account linked to it
// has sufficient permissions for consuming messages.
func doSubscriptionHealthCheck(ctx context.Context, subscription *pubsub.Subscription) error {
	exists, err := subscription.Exists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		return errors.Errorf("given subscription %s does not exist", subscription.ID())
	}

	return testPermissions(ctx, subscription.IAM(), []string{"pubsub.subscriptions.consume"})
}

func (p *Receiver) Close() error {
	return p.client.Close()
}

func (p *Receiver) Receive(ctx context.Context, f func(context.Context, broker.Message)) error {
	return p.subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		f(ctx, intoBrokerMessage(message))
	})
}

func intoBrokerMessage(message *pubsub.Message) broker.Message {
	var attributes map[string]interface{}
	if message.Attributes != nil {
		attributes = make(map[string]interface{}, len(message.Attributes))
		for k, v := range message.Attributes {
			attributes[k] = v
		}
	}

	return broker.Message{
		ID:            message.ID,
		Attributes:    attributes,
		Data:          message.Data,
		AckFunc:       message.Ack,
		NackFunc:      message.Nack,
		PublishTime:   message.PublishTime,
		IngestionTime: time.Now(),
	}
}
