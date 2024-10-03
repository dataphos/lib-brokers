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

package jetstream

import (
	"context"
	"github.com/nats-io/nats.go"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

type Iterator struct {
	config       IteratorConfig
	subscription *nats.Subscription
}

type IteratorConfig struct {
	URL          string
	Subject      string
	ConsumerName string
}

func NewIterator(config IteratorConfig) (*Iterator, error) {
	js, err := setupJetstreamConnection(config.URL)
	if err != nil {
		return nil, err
	}

	subscription, err := js.PullSubscribe(
		config.Subject,
		config.ConsumerName,
		nats.DeliverAll(),
		nats.AckExplicit())
	if err != nil {
		return nil, err
	}

	return &Iterator{
		config:       config,
		subscription: subscription,
	}, nil
}

func (it *Iterator) NextMessage(ctx context.Context) (broker.Message, error) {
	if ctx == context.Background() {
		var cancel context.CancelFunc
		// The nats client returns an error in case an empty context (context.Background) is passed onto it.
		// We believe this is a bad design choice, so we mitigate it by wrapping the empty context with 500 milliseconds
		// deadline to mimic Kafka's poll timeout. In case there are no messages received the server will block for 500
		// milliseconds before answering the fetch request.
		// See https://github.com/nats-io/nats.go/lob/v1.16.0/js.go#L2543-L2552 for reason why.
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Millisecond*time.Duration(500)))
		defer cancel()
	}

	message, err := it.subscription.Fetch(1, nats.Context(ctx)) //nolint:contextcheck // we are passing nats context
	if err != nil {
		return broker.Message{}, err
	}

	return intoBrokerMessage(message[0]), nil
}

func (it *Iterator) Close() {
	_ = it.subscription.Unsubscribe()
}

type BatchIterator struct {
	subscription *nats.Subscription
	settings     BatchIteratorSettings
}

type BatchIteratorSettings struct {
	BatchSize int
}

var DefaultBatchIteratorSettings = BatchIteratorSettings{
	BatchSize: 100,
}

func NewBatchIterator(ctx context.Context, config IteratorConfig, settings BatchIteratorSettings) (*BatchIterator, error) {
	js, err := setupJetstreamConnection(config.URL)
	if err != nil {
		return nil, err
	}

	subscription, err := js.PullSubscribe(
		config.Subject,
		config.ConsumerName,
		nats.Context(ctx),
		nats.DeliverAll(),
		nats.MaxRequestBatch(settings.BatchSize),
		nats.AckExplicit(),
		nats.ReplayInstant(),
	)
	if err != nil {
		return nil, err
	}

	return &BatchIterator{
		subscription: subscription,
		settings:     settings,
	}, nil
}

func (it *BatchIterator) NextBatch(ctx context.Context) ([]broker.Message, error) {
	if ctx == context.Background() {
		var cancel context.CancelFunc
		// The nats client returns an error in case an empty context (context.Background) is passed onto it.
		// We believe this is a bad design choice, so we mitigate it by wrapping the empty context with 500 milliseconds
		// deadline to mimic Kafka's poll timeout. In case there are no messages received the server will block for 500
		// milliseconds before answering the fetch request.
		// See https://github.com/nats-io/nats.go/lob/v1.16.0/js.go#L2543-L2552 for reason why.
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(time.Millisecond*time.Duration(500)))
		defer cancel()
	}

	messages, err := it.subscription.Fetch(it.settings.BatchSize, nats.Context(ctx)) //nolint:contextcheck // we are passing nats context
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			if ctx.Err() == nil {
				return nil, nil
			}
		}

		return nil, err
	}

	parsed := make([]broker.Message, len(messages))
	for i := range messages {
		parsed[i] = intoBrokerMessage(messages[i])
	}

	return parsed, nil
}

func setupJetstreamConnection(url string) (nats.JetStream, error) { //nolint:ireturn //have to return interface
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	return conn.JetStream()
}

func intoBrokerMessage(message *nats.Msg) broker.Message {
	// Safe to ignore the error since this is a jetstream message.
	metadata, _ := message.Metadata()
	sequencePair := metadata.Sequence

	var headers map[string]interface{}
	if message.Header != nil {
		headers = make(map[string]interface{}, len(message.Header))
		for k, v := range message.Header {
			headers[k] = v
		}
	}

	return broker.Message{
		ID:            strconv.FormatUint(sequencePair.Consumer, 10),
		Data:          message.Data,
		Attributes:    headers,
		PublishTime:   metadata.Timestamp,
		IngestionTime: time.Now(),
		AckFunc: func() {
			_ = message.AckSync()
		},
		NackFunc: func() {
			_ = message.Nak()
		},
	}
}

func (it *BatchIterator) Close() {
	_ = it.subscription.Unsubscribe()
}
