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
	"github.com/dataphos/lib-brokers/pkg/broker/pulsar"
	"github.com/dataphos/lib-httputil/pkg/httputil"
)

func main() {
	publishWithTLS()
}

func publishMessage() {
	publisher, err := pulsar.NewPublisher(pulsar.PublisherConfig{
		ServiceURL: "pulsar://localhost:6650",
		TLSConfig:  nil,
	}, pulsar.DefaultPublisherSettings)
	if err != nil {
		log.Fatal(err)
	}

	topic, err := publisher.Topic("create.test.topic2")
	if err != nil {
		log.Fatal(err)
	}

	if err := topic.Publish(context.Background(), broker.OutboundMessage{Data: []byte("some data")}); err != nil {
		log.Fatal(err)
	}

	log.Println("Message published successfully")

	if err := publisher.Close(); err != nil {
		log.Fatal(err)
	}
}

func publishWithTLS() {
	clientCertPath := "my-ca/certs/client.cert.pem"
	clientKeyPath := "my-ca/private/client.key.pem"
	caCertPath := "my-ca/certs/ca.cert.pem"

	tlsConfig, err := httputil.NewTLSConfig(clientCertPath, clientKeyPath, caCertPath)
	if err != nil {
		log.Fatal(err)
	}

	settings := pulsar.DefaultPublisherSettings
	settings.TLSTrustCertsFilePath = caCertPath
	settings.TLSAllowInsecureConnection = true

	publisher, err := pulsar.NewPublisher(pulsar.PublisherConfig{
		ServiceURL: "pulsar+ssl://localhost:6651",
		TLSConfig:  tlsConfig,
	}, settings)
	if err != nil {
		log.Fatal(err)
	}

	topic, err := publisher.Topic("create.test.topic2")
	if err != nil {
		log.Fatal(err)
	}

	if err := topic.Publish(context.Background(), broker.OutboundMessage{Data: []byte("some data")}); err != nil {
		log.Fatal(err)
	}

	log.Println("Message published successfully")

	if err := publisher.Close(); err != nil {
		log.Fatal(err)
	}
}
