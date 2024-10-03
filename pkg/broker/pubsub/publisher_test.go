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

package pubsub_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dataphos/lib-brokers/pkg/broker"
	ps "github.com/dataphos/lib-brokers/pkg/broker/pubsub"
)

func TestNewPublisher(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "new-client-test"

	settings := ps.DefaultPublishSettings
	config := ps.PublisherConfig{
		ProjectID:      projectID,
		GrpcClientConn: conn,
	}

	client, err := ps.NewPublisher(context.Background(), config, settings)
	if err != nil {
		t.Fatal("Error while creating client: ", err)
	}

	err = client.Close()
	if err != nil {
		t.Fatal("Error while closing client: ", err)
	}
}

func TestPublishOneMessage(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "publish-one-message-test"
	topicID := "publish-test"

	settings := ps.DefaultPublishSettings
	config := ps.PublisherConfig{
		ProjectID:      projectID,
		GrpcClientConn: conn,
	}

	var opts []option.ClientOption
	opts = append(opts, option.WithGRPCConn(config.GrpcClientConn))
	client, _ := pubsub.NewClient(context.Background(), config.ProjectID, opts...)

	publisher, err := ps.NewPublisher(context.Background(), config, settings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := client.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	message := broker.OutboundMessage{
		Key:        "key1",
		Data:       []byte("Simple message."),
		Attributes: nil,
	}

	topic, err := publisher.Topic(topicID)
	if err != nil {
		t.Fatal("Error while referencing topic: ", err)
	}

	subscription, err := client.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic: createdTopic,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	err = topic.Publish(context.Background(), message)
	if err != nil {
		t.Fatal("Error while publishing message: ", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receivedMessages := make(chan pubsub.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(receivedMessages)
		defer close(errc)

		if err := subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			receivedMessages <- *message
		}); err != nil {
			errc <- err
		}
	}()

	results, errs := collectPubsubMessagesAndErrors(receivedMessages, errc)

	if len(errs) > 0 {
		t.Fatal("errors occurred", errs)
	}

	if len(results) != 1 {
		t.Fatal("didn't pull the published message")
	}

	if !reflect.DeepEqual(message.Data, results[0].Data) {
		t.Fatal("expected and actual message not the same")
	}
}

func TestPublishMoreMessages(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "publish-more-messages-test"
	topicID := "publish-more-messages"

	settings := ps.DefaultPublishSettings
	config := ps.PublisherConfig{
		ProjectID:      projectID,
		GrpcClientConn: conn,
	}

	var opts []option.ClientOption
	opts = append(opts, option.WithGRPCConn(config.GrpcClientConn))
	client, _ := pubsub.NewClient(context.Background(), config.ProjectID, opts...)

	publisher, err := ps.NewPublisher(context.Background(), config, settings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := client.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	topic, err := publisher.Topic(topicID)
	if err != nil {
		t.Fatal("Error while referencing topic: ", err)
	}

	messages := []broker.OutboundMessage{
		{
			Key:        "key1",
			Data:       []byte("First message."),
			Attributes: nil,
		},
		{
			Key:        "key2",
			Data:       []byte("Second message."),
			Attributes: nil,
		},
		{
			Key:        "key3",
			Data:       []byte("Third message."),
			Attributes: nil,
		},
	}

	subscription, err := client.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic: createdTopic,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	for _, message := range messages {
		err = topic.Publish(context.Background(), message)
		if err != nil {
			t.Fatal("Error while publishing message: ", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receivedMessages := make(chan pubsub.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(receivedMessages)
		defer close(errc)

		if err := subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			receivedMessages <- *message
		}); err != nil {
			errc <- err
		}
	}()

	results, errs := collectPubsubMessagesAndErrors(receivedMessages, errc)

	if len(errs) > 0 {
		t.Fatal("errors occurred", errs)
	}

	if len(results) != 3 {
		t.Fatal("didn't pull the published message")
	}

	var received bool
	for _, message := range messages {
		received = false

		for _, result := range results {
			if reflect.DeepEqual(message.Data, result.Data) {
				received = true
			}
		}

		if !received {
			t.Fatalf("Message %s not received", string(message.Data))
		}
	}
}

func TestPublishMoreMessagesOrdered(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "publish-more-messages-ordered-test"
	topicID := "publish-more-messages-ordered"

	settings := ps.DefaultPublishSettings
	settings.EnableMessageOrdering = true
	config := ps.PublisherConfig{
		ProjectID:      projectID,
		GrpcClientConn: conn,
	}

	var opts []option.ClientOption
	opts = append(opts, option.WithGRPCConn(config.GrpcClientConn))
	client, _ := pubsub.NewClient(context.Background(), config.ProjectID, opts...)

	publisher, err := ps.NewPublisher(context.Background(), config, settings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := client.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	createdTopic.EnableMessageOrdering = true

	topic, err := publisher.Topic(topicID)
	if err != nil {
		t.Fatal("Error while referencing topic: ", err)
	}

	messages := []broker.OutboundMessage{
		{
			Key:        "key",
			Data:       []byte("First message."),
			Attributes: nil,
		},
		{
			Key:        "key",
			Data:       []byte("Second message."),
			Attributes: nil,
		},
		{
			Key:        "key",
			Data:       []byte("Third message."),
			Attributes: nil,
		},
	}

	subscription, err := client.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic:                 createdTopic,
		EnableMessageOrdering: true,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	for _, message := range messages {
		err = topic.Publish(context.Background(), message)
		if err != nil {
			t.Fatal("Error while publishing message: ", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receivedMessages := make(chan pubsub.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(receivedMessages)
		defer close(errc)

		if err := subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			receivedMessages <- *message
		}); err != nil {
			errc <- err
		}
	}()

	results, errs := collectPubsubMessagesAndErrors(receivedMessages, errc)

	if len(errs) > 0 {
		t.Fatal("errors occurred", errs)
	}

	if len(results) != 3 {
		t.Fatal("didn't pull the published message")
	}

	for index, result := range results {
		if !reflect.DeepEqual(messages[index].Data, result.Data) {
			t.Fatalf("Messages are not ordered properly. Expected message: %s , got message %s", messages[index].Data, result.Data)
		}
	}
}

func TestPublishBatch(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "publish-more-messages-test"
	topicID := "publish-more-messages"

	settings := ps.DefaultPublishSettings
	config := ps.PublisherConfig{
		ProjectID:      projectID,
		GrpcClientConn: conn,
	}

	var opts []option.ClientOption
	opts = append(opts, option.WithGRPCConn(config.GrpcClientConn))
	client, _ := pubsub.NewClient(context.Background(), config.ProjectID, opts...)

	publisher, err := ps.NewPublisher(context.Background(), config, settings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := client.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	topic, err := publisher.Topic(topicID)
	if err != nil {
		t.Fatal("Error while referencing topic: ", err)
	}

	numMsgs := 1000
	messages := make([]broker.OutboundMessage, 0, numMsgs)

	for i := 0; i < numMsgs; i++ {
		messages = append(messages, broker.OutboundMessage{
			Key:        "key",
			Data:       []byte(strconv.Itoa(i)),
			Attributes: nil,
		})
	}

	subscription, err := client.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic: createdTopic,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	err = topic.BatchPublish(context.Background(), messages...)
	if err != nil {
		t.Fatal("Error while publishing message: ", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	receivedMessages := make(chan pubsub.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(receivedMessages)
		defer close(errc)

		if err := subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			receivedMessages <- *message
		}); err != nil {
			errc <- err
		}
	}()

	results, errs := collectPubsubMessagesAndErrors(receivedMessages, errc)

	if len(errs) > 0 {
		t.Fatal("errors occurred", errs)
	}

	if len(results) != numMsgs {
		t.Fatalf("didn't pull the published messages: only %d/%d", len(results), numMsgs)
	}

	// Make a map for fast searching.
	payloads := messagePayloads(results)

	for _, message := range messages {
		dataString := string(message.Data)

		if _, found := payloads[dataString]; !found {
			t.Fatalf("Message %s not received", dataString)
		}
	}
}

func messagePayloads(messages []pubsub.Message) map[string]struct{} {
	payloads := map[string]struct{}{}
	for _, msg := range messages {
		payloads[string(msg.Data)] = struct{}{}
	}

	return payloads
}

func collectPubsubMessagesAndErrors(receivedMessages chan pubsub.Message, errc chan error) ([]pubsub.Message, []error) {
	resultsDrain := make(chan []pubsub.Message)

	go func() {
		defer close(resultsDrain)

		var results []pubsub.Message

		for r := range receivedMessages {
			r.Ack()
			results = append(results, r)
		}
		resultsDrain <- results
	}()

	errorDrain := make(chan []error)

	go func() {
		defer close(errorDrain)

		var errs []error

		for r := range errc {
			errs = append(errs, r)
		}
		errorDrain <- errs
	}()

	results := <-resultsDrain
	errs := <-errorDrain

	return results, errs
}
