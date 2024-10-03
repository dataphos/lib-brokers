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

package broker

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Publisher models a message publishing operation.
type Publisher interface {
	// Topic constructs a new Topic.
	Topic(string) (Topic, error)
}

type TransactionalPublisher interface {
	Publisher
	Begin() error
	Commit(ctx context.Context) (bool, error)
	Abort(ctx context.Context) (bool, error)
}

// Topic defines a topic object.
type Topic interface {
	// Publish publishes a Message.
	//
	// Blocks until the Message is received by the broker, the context is canceled or an error occurs.
	Publish(context.Context, OutboundMessage) error
	// BatchPublish publishes a batch.
	//
	// Blocks until all the messages in the batch are received by the broker, the context is canceled or an error occurs.
	BatchPublish(context.Context, ...OutboundMessage) error
}

// OutboundMessage models a message that gets sent to a broker.Topic.
type OutboundMessage struct {
	// Key identifies the Data part of the message (and not the message itself).
	// It doesn't have to be unique across the topic and is mostly used for ordering guarantees.
	Key string

	// Data the payload of the message.
	Data []byte

	// Attributes holds the properties commonly used by brokers to pass metadata.
	Attributes map[string]interface{}
}

// SimplePublisherParallelization publishes a batch of messages in parallel and is used if no specific batching
// optimization of the underlying Publisher is implemented.
func SimplePublisherParallelization(ctx context.Context, topic Topic, messages ...OutboundMessage) error {
	errorGroup, groupCtx := errgroup.WithContext(ctx)

	for _, loopMsg := range messages {
		msg := loopMsg

		errorGroup.Go(func() error {
			return topic.Publish(groupCtx, msg)
		})
	}

	return errorGroup.Wait()
}
