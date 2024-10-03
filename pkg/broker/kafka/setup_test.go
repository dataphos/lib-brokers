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

// Not actually a test file, but a setup file for all tests that require a kafka broker.
package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
)

const (
	TimeoutDuration    = time.Second * 10
	ZookeeperImageName = "bitnami/zookeeper"
	ZookeeperImageTag  = "latest"
	KafkaImageName     = "bitnami/kafka"
	KafkaImageTag      = "latest"
	BrokerPort         = "9092"
	BrokerAddrs        = "localhost:" + BrokerPort
)

var (
	dockerPool        *dockertest.Pool
	zookeeperResource *dockertest.Resource
	kafkaResource     *dockertest.Resource
	dockerNetwork     *docker.Network

	producerConfig   kafka.ProducerConfig
	producerSettings = kafka.DefaultProducerSettings

	consumerSettings = kafka.ConsumerSettings{
		// MinBytes to 1 because test messages are small.
		MinBytes:             1,
		MaxWait:              5 * time.Second,
		MaxBytes:             10 * 1024 * 1024,
		MaxConcurrentFetches: 1,
	}
	batchConsumerSettings = kafka.BatchConsumerSettings{
		ConsumerSettings: consumerSettings,
		MaxPollRecords:   2,
	}
)

var errnotconn = errors.New("not yet connected")

func setup() {
	start := time.Now()

	log.Println("Setting up testing environment...")
	// Set up connection to docker daemon.
	var err error

	dockerPool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	if err = dockerPool.Client.Ping(); err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}
	// Create a docker bridge network for the containers.
	dockerNetwork, err = dockerPool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: "zookeeper_kafka_network"})
	if err != nil {
		log.Fatalf("could not create a network to zookeeper and kafka: %s", err)
	}
	// Configure a Zookeeper container to run.
	zookeeperResource, err = dockerPool.RunWithOptions(&dockertest.RunOptions{
		Name:         "zookeeper-unit-testing",
		Repository:   ZookeeperImageName,
		Tag:          ZookeeperImageTag,
		NetworkID:    dockerNetwork.ID,
		Hostname:     "zookeeper",
		ExposedPorts: []string{"2181"},
		Env:          []string{"ALLOW_ANONYMOUS_LOGIN=yes"},
	})
	if err != nil {
		log.Fatalf("could not start zookeeper: %s", err)
	}

	conn, _, err := zk.Connect([]string{fmt.Sprintf("127.0.0.1:%s", zookeeperResource.GetPort("2181/tcp"))}, 10*time.Second)
	if err != nil {
		log.Fatalf("could not connect zookeeper: %s", err)
	}
	defer conn.Close()

	retryFn := func() error {
		switch conn.State() {
		case zk.StateHasSession, zk.StateConnected:
			return nil
		case zk.StateConnecting,
			zk.StateConnectedReadOnly,
			zk.StateAuthFailed,
			zk.StateSaslAuthenticated,
			zk.StateUnknown,
			zk.StateExpired,
			zk.StateDisconnected:
			return errnotconn
		default:
			return errnotconn
		}
	}
	// Wait for the Zookeeper to be available before starting a Kafka container.
	if err = dockerPool.Retry(retryFn); err != nil {
		log.Fatalf("could not connect to zookeeper: %s", err)
	}

	// Configure a Kafka container to run.
	kafkaResource, err = dockerPool.RunWithOptions(&dockertest.RunOptions{
		Name:       "kafka-unit-testing",
		Repository: KafkaImageName,
		Tag:        KafkaImageTag,
		NetworkID:  dockerNetwork.ID,
		Hostname:   "kafka",
		Env: []string{
			"ALLOW_PLAINTEXT_LISTENER=yes",
			"KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true",
			"KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://" + BrokerAddrs,
			"KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:" + BrokerPort,
			"KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			BrokerPort + "/tcp": {{HostIP: "localhost", HostPort: BrokerPort + "/tcp"}},
		},
		ExposedPorts: []string{BrokerPort + "/tcp"},
	})
	if err != nil {
		log.Fatalf("could not start kafka: %s", err)
	}

	// For local testing, this is how CPU can be limited to "approximate" GitHub Actions resources.
	// This limits the broker container to use 40% of a single core, meaning 10% of total CPU resources (4 cores).
	// The minimum for both CPUQuota and CPUPeriod are 1000, which is why they are set in thousands.
	//err = dockerPool.Client.UpdateContainer(kafkaResource.Container.ID, docker.UpdateContainerOptions{
	//	CPUQuota:  4000,
	//	CPUPeriod: 10000,
	//})
	//if err != nil {
	//	log.Fatal(err)
	//}  //nolint:godot // no need to end this comment with period.

	// Wait for the kafka broker to be available.
	kgoClient, err := kgo.NewClient(kgo.SeedBrokers(BrokerAddrs))
	if err != nil {
		log.Fatal(err)
	}
	defer kgoClient.Close()

	retryFn = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
		defer cancel()

		return kgoClient.Ping(ctx)
	}
	if err = dockerPool.Retry(retryFn); err != nil {
		log.Fatalf("could not connect to kafka: %s", err)
	}

	// Configure producer.
	producerConfig = kafka.ProducerConfig{BrokerAddr: BrokerAddrs}
	elapsed := time.Since(start)
	log.Printf("Setup took %s\n", elapsed)
}

func teardown() {
	log.Println("Tearing down testing environment...")

	if err := dockerPool.Purge(zookeeperResource); err != nil {
		log.Fatalf("could not purge zookeeperResource: %s", err)
	}

	if err := dockerPool.Purge(kafkaResource); err != nil {
		log.Fatalf("could not purge kafkaResource: %s", err)
	}

	if err := dockerPool.Client.RemoveNetwork(dockerNetwork.ID); err != nil {
		log.Fatalf("could not remove %s network: %s", dockerNetwork.Name, err)
	}
}

func TestMain(m *testing.M) {
	setup()

	code := m.Run()

	teardown()
	os.Exit(code)
}
