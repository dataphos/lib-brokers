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
	"fmt"

	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

type Publisher struct {
	conn   *nats.Conn
	client nats.JetStream
}

type PublisherSettings struct {
	MaxPending int
}

var DefaultPublisherSettings = PublisherSettings{
	MaxPending: 512,
}

func NewPublisher(ctx context.Context, url string, settings PublisherSettings) (*Publisher, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	jetStream, err := conn.JetStream(
		nats.Context(ctx),
		nats.PublishAsyncMaxPending(settings.MaxPending),
	)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client: jetStream,
	}, nil
}

func (p *Publisher) Topic(topic string) (broker.Topic, error) {
	return &Topic{
		topic:  topic,
		client: p.client,
	}, nil
}

func (p *Publisher) Close() error {
	p.conn.Close()

	return nil
}

type Topic struct {
	topic  string
	client nats.JetStream
}

func (t *Topic) Publish(ctx context.Context, message broker.OutboundMessage) error {
	if _, err := t.client.PublishMsg(
		t.intoNatsMessage(message),
		nats.Context(ctx),
	); err != nil {
		return err
	}

	return nil
}

func (t *Topic) BatchPublish(ctx context.Context, messages ...broker.OutboundMessage) error {
	errorGroup, groupCtx := errgroup.WithContext(ctx)
	nCtx := nats.Context(groupCtx)

	for _, loopMsg := range messages {
		msg := loopMsg

		errorGroup.Go(func() error {
			_, err := t.client.PublishMsg(t.intoNatsMessage(msg), nCtx)

			return err
		})
	}

	return errorGroup.Wait()
}

func (t *Topic) intoNatsMessage(message broker.OutboundMessage) *nats.Msg {
	var headers map[string][]string
	if message.Attributes != nil {
		headers = make(map[string][]string, len(message.Attributes))

		for key, value := range message.Attributes {
			if s, ok := value.([]string); ok {
				headers[key] = s
			} else {
				// For now, we're going to assume only string slices are valid attributes, so whatever remains,
				// we just cast into a single-element string slice.
				headers[key] = []string{fmt.Sprintf("%s", value)}
			}
		}
	}

	return &nats.Msg{
		Subject: t.topic,
		Header:  headers,
		Data:    message.Data,
	}
}
