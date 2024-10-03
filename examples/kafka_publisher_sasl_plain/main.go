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

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
)

func main() {
	publisher, err := kafka.NewPublisher(
		context.Background(),
		kafka.ProducerConfig{
			BrokerAddr: "BrokerAddress:9093",
			PlainSASL: &kafka.PlainSASLConfig{
				User: "$ConnectionString",
				Pass: "Password",
			},
			DisableCompression: true,
		},
		kafka.DefaultProducerSettings,
	)
	if err != nil {
		log.Fatal(err)
	}
	topic, err := publisher.Topic("")
	if err != nil {
		log.Fatal(err)
	}

	if err = topic.Publish(context.Background(), broker.OutboundMessage{
		Data: []byte("some data"),
	}); err != nil {
		log.Fatal(err)
	}
}
