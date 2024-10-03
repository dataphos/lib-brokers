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

	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
)

func main() {
	consumer, err := kafka.NewIterator(
		context.Background(),
		kafka.ConsumerConfig{
			BrokerAddr: "BrokerAddress:9093",
			GroupID:    "",
			Topic:      "",
			PlainSASL: &kafka.PlainSASLConfig{
				User: "$ConnectionString",
				Pass: "Password",
			},
		},
		kafka.DefaultConsumerSettings,
	)
	if err != nil {
		log.Fatal(err)
	}

	record, err := consumer.NextRecord(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Record: %s\n", record.Value)
	record.Ack()
}
