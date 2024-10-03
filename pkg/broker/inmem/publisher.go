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

// Package inmem is an in-memory, "dummy" implementation of the broker API.
package inmem

import (
	"context"
	"reflect"
	"sync"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

type Publisher struct {
	Spawned []*Topic
	mu      sync.Mutex
}

func (p *Publisher) Topic(topicID string) (broker.Topic, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	topic := &Topic{TopicID: topicID}

	p.Spawned = append(p.Spawned, topic)

	return topic, nil
}

type Topic struct {
	TopicID   string
	Published []broker.OutboundMessage
	mu        sync.Mutex
}

func (m *Topic) Publish(_ context.Context, message broker.OutboundMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Published = append(m.Published, message)

	return nil
}

func (m *Topic) BatchPublish(_ context.Context, messages ...broker.OutboundMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Published = append(m.Published, messages...)

	return nil
}

func (p *Publisher) MatchesPublished(expected ...broker.Message) bool {
	actual := p.GetAllPublished()

	if len(actual) != len(expected) {
		return false
	}

	for _, actualMsg := range actual {
		foundSame := false

		for _, expectedMsg := range expected {
			if reflect.DeepEqual(actualMsg, expectedMsg) {
				foundSame = true

				break
			}
		}

		if !foundSame {
			return false
		}
	}

	return true
}

func (p *Publisher) GetAllPublished() []broker.OutboundMessage {
	var published []broker.OutboundMessage

	for _, topic := range p.Spawned {
		published = append(published, topic.Published...)
	}

	return published
}
