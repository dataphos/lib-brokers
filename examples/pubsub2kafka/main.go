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

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
	"github.com/dataphos/lib-brokers/pkg/broker/pubsub"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

		<-c

		cancel()
	}()

	receiver, err := pubsub.NewReceiver(
		ctx,
		pubsub.ReceiverConfig{
			ProjectID:      "",
			SubscriptionID: "",
		},
		pubsub.DefaultReceiveSettings,
	)
	if err != nil {
		log.Fatal(err)
	}

	publisher, err := kafka.NewPublisher(
		context.Background(),
		kafka.ProducerConfig{
			BrokerAddr: "localhost:9092",
		},
		kafka.DefaultProducerSettings,
	)
	if err != nil {
		log.Fatal(err)
	}
	smallMessageTopic, err := publisher.Topic("small.message.topic")
	if err != nil {
		log.Fatal(err)
	}
	largeMessageTopic, err := publisher.Topic("large.message.topic")
	if err != nil {
		log.Fatal(err)
	}

	if err = receiver.Receive(ctx, func(ctx context.Context, message broker.Message) {
		var targetTopic broker.Topic
		if len(message.Data) < 10*1024 {
			targetTopic = smallMessageTopic
		} else {
			targetTopic = largeMessageTopic
		}

		if err := targetTopic.Publish(ctx, broker.OutboundMessage{
			Key:        message.Key,
			Data:       message.Data,
			Attributes: message.Attributes,
		}); err != nil {
			cancel()
		}
	}); err != nil {
		log.Fatal(err)
	}
}
