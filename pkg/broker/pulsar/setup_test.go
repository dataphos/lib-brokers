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

package pulsar_test

import (
	"context"
	"github.com/ory/dockertest/v3"
	"log"
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ory/dockertest/v3/docker"

	p "github.com/dataphos/lib-brokers/pkg/broker/pulsar"
)

const (
	TimeoutDuration  = 10 * time.Second
	PulsarImageName  = "apachepulsar/pulsar"
	PulsarImageTag   = "2.10.2"
	BrokerPort       = "6650"
	BrokerServerPort = "8080"
	ServiceHostname  = "localhost"
	ServiceURL       = "pulsar://" + ServiceHostname + ":" + BrokerPort
)

var (
	dockerPool           *dockertest.Pool
	pulsarResource       *dockertest.Resource
	maxReconnectToBroker = uint(3)
)

func setup() {
	// Set up connection to docker daemon.
	var err error

	dockerPool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	// Pulsar needs longer to start. Default is 1 minute and it is too short.
	dockerPool.MaxWait = 120 * time.Second

	if err = dockerPool.Client.Ping(); err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	// configure Pulsar broker to run in standalone mode.
	pulsarResource, err = dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository:   PulsarImageName,
		Tag:          PulsarImageTag,
		Entrypoint:   []string{"bin/pulsar", "standalone"},
		ExposedPorts: []string{BrokerPort + "/tcp", BrokerServerPort + "/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			BrokerPort + "/tcp":       {{HostIP: ServiceHostname, HostPort: BrokerPort + "/tcp"}},
			BrokerServerPort + "/tcp": {{HostIP: ServiceHostname, HostPort: BrokerServerPort + "/tcp"}},
		},
	})
	if err != nil {
		log.Fatalf("could not start pulsar: %v", err)
	}

	// Wait for the pulsar broker to be available.
	retryFn := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
		defer cancel()
		// try to publish a message.
		client, err := pulsar.NewClient(pulsar.ClientOptions{URL: ServiceURL})
		if err != nil {
			return err
		}

		producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: "setup.test.topic"})
		if err != nil {
			return err
		}

		_, err = producer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte("Test message")})

		return err
	}

	if err := dockerPool.Retry(retryFn); err != nil {
		log.Fatalf("could not connect to pulsar: %v", err)
	}

	// configure publisher settings.
	publisherSettings = p.DefaultPublisherSettings
	publisherSettings.MaxReconnectToBroker = &maxReconnectToBroker
}

func teardown() {
	log.Println("Tearing down test environment...")

	if err := dockerPool.Purge(pulsarResource); err != nil {
		log.Fatalf("could not purge pulsar resource: %v", err)
	}
}

func TestMain(m *testing.M) {
	setup()

	code := m.Run()

	teardown()
	os.Exit(code)
}
