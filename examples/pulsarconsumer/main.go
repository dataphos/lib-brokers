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

	"github.com/dataphos/lib-brokers/pkg/broker/pulsar"
	"github.com/dataphos/lib-httputil/pkg/httputil"
)

func main() {
	pullWithTLS()
}

func pullMessage() {
	settings := pulsar.DefaultIteratorSettings
	settings.SubscriptionType = pulsar.Shared
	settings.ReceiverQueueSize = 10

	consumer, err := pulsar.NewIterator(pulsar.IteratorConfig{
		ServiceURL:   "pulsar://localhost:6650",
		Topic:        "create.test.topic2",
		Subscription: "create.test.sub2",
	}, settings)
	if err != nil {
		log.Fatal(err)
	}

	msg, err := consumer.NextMessage(context.Background())
	if err != nil {
		msg.Nack()
		log.Fatal(err)
	}

	log.Printf("Received message msgId: %#v -- content: '%s'\n",
		msg.ID, string(msg.Data))
	msg.Ack()

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
}

func pullWithTLS() {
	clientCertPath := "my-ca/certs/client.cert.pem"
	clientKeyPath := "my-ca/private/client.key.pem"
	caCertPath := "my-ca/certs/ca.cert.pem"

	tlsConfig, err := httputil.NewTLSConfig(clientCertPath, clientKeyPath, caCertPath)
	if err != nil {
		log.Fatal(err)
	}

	settings := pulsar.DefaultIteratorSettings
	settings.SubscriptionType = pulsar.Shared
	settings.ReceiverQueueSize = 10
	settings.TLSAllowInsecureConnection = true

	consumer, err := pulsar.NewIterator(pulsar.IteratorConfig{
		ServiceURL:   "pulsar+ssl://localhost:6651",
		TLSConfig:    tlsConfig,
		Topic:        "create.test.topic2",
		Subscription: "create.test.sub2",
	}, settings)
	if err != nil {
		log.Fatal(err)
	}

	msg, err := consumer.NextMessage(context.Background())
	if err != nil {
		msg.Nack()
		log.Fatal(err)
	}

	log.Printf("Received message msgId: %#v -- content: '%s'\n",
		msg.ID, string(msg.Data))
	msg.Ack()

	if err := consumer.Close(); err != nil {
		log.Fatal(err)
	}
}
