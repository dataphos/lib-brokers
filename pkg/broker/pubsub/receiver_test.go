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

func TestNewReceiver(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()

	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "new-receiver-test"
	topicID := "new-receiver"

	publisher, err := pubsub.NewClient(context.Background(), projectID, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := publisher.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	subscription, err := publisher.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic: createdTopic,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	config := ps.ReceiverConfig{
		GrpcClientConn: conn,
		SubscriptionID: subscription.ID(),
		ProjectID:      projectID,
	}
	settings := ps.DefaultReceiveSettings

	receiver, err := ps.NewReceiver(context.Background(), config, settings)
	if err != nil {
		t.Fatal("Error while creating new receiver: ", err)
	}

	err = receiver.Close()
	if err != nil {
		t.Fatal("Error while closing receiver: ", err)
	}
}

func TestReceiveOneMessage(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "receive-one-message-test"
	topicID := "receive-one-message"

	publisher, err := pubsub.NewClient(context.Background(), projectID, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := publisher.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	message := &pubsub.Message{
		Data:       []byte("Simple message."),
		Attributes: nil,
	}

	subscription, err := publisher.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic: createdTopic,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	r := createdTopic.Publish(context.Background(), message)

	_, err = r.Get(context.Background())
	if err != nil {
		t.Fatal("Error while publishing message: ", err)
	}

	config := ps.ReceiverConfig{
		ProjectID:      projectID,
		SubscriptionID: subscription.ID(),
		GrpcClientConn: conn,
	}
	settings := ps.DefaultReceiveSettings

	receiver, _ := ps.NewReceiver(context.Background(), config, settings)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receivedMessages := make(chan broker.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(receivedMessages)
		defer close(errc)

		if err := receiver.Receive(ctx, func(ctx context.Context, message broker.Message) {
			receivedMessages <- message
		}); err != nil {
			errc <- err
		}
	}()

	results, errs := collectBrokerMessagesAndErrors(receivedMessages, errc)

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

func TestReceiveMoreMessages(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "receive-more-messages-test"
	topicID := "receive-more-messages"

	publisher, err := pubsub.NewClient(context.Background(), projectID, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := publisher.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	messages := []pubsub.Message{
		{
			Data:       []byte("First message."),
			Attributes: nil,
		},
		{
			Data:       []byte("Second message."),
			Attributes: nil,
		},
		{
			Data:       []byte("Third message."),
			Attributes: nil,
		},
	}

	subscription, err := publisher.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic: createdTopic,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	for _, message := range messages {
		message := message
		r := createdTopic.Publish(context.Background(), &message)
		_, err := r.Get(context.Background())
		if err != nil { //nolint:wsl //file gofumpt-ed
			t.Fatal("Error while publishing message: ", err)
		}
	}

	config := ps.ReceiverConfig{
		ProjectID:      projectID,
		SubscriptionID: subscription.ID(),
		GrpcClientConn: conn,
	}
	settings := ps.DefaultReceiveSettings

	receiver, _ := ps.NewReceiver(context.Background(), config, settings)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receivedMessages := make(chan broker.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(receivedMessages)
		defer close(errc)

		if err := receiver.Receive(ctx, func(ctx context.Context, message broker.Message) {
			receivedMessages <- message
		}); err != nil {
			errc <- err
		}
	}()

	results, errs := collectBrokerMessagesAndErrors(receivedMessages, errc)

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

func TestReceiveMoreMessagesOrdered(t *testing.T) {
	t.Parallel()

	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { //nolint:wsl //file gofumpt-ed
		t.Fatal(err)
	}
	defer conn.Close()

	projectID := "receive-more-messages-ordered-test"
	topicID := "receive-more-messages-ordered"

	publisher, err := pubsub.NewClient(context.Background(), projectID, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	createdTopic, err := publisher.CreateTopic(context.Background(), topicID)
	if err != nil {
		t.Fatal("Error while creating topic: ", err)
	}

	createdTopic.EnableMessageOrdering = true

	messages := []pubsub.Message{
		{
			OrderingKey: "key",
			Data:        []byte("First message."),
			Attributes:  nil,
		},
		{
			OrderingKey: "key",
			Data:        []byte("Second message."),
			Attributes:  nil,
		},
		{
			OrderingKey: "key",
			Data:        []byte("Third message."),
			Attributes:  nil,
		},
	}

	subscription, err := publisher.CreateSubscription(context.Background(), projectID, pubsub.SubscriptionConfig{
		Topic:                 createdTopic,
		EnableMessageOrdering: true,
	})
	if err != nil {
		t.Fatal("Error while creating subscription: ", err)
	}

	for _, message := range messages {
		message := message
		r := createdTopic.Publish(context.Background(), &message)
		_, err := r.Get(context.Background())
		if err != nil { //nolint:wsl //file gofumpt-ed
			t.Fatal("Error while publishing message: ", err)
		}
	}

	config := ps.ReceiverConfig{
		ProjectID:      projectID,
		SubscriptionID: subscription.ID(),
		GrpcClientConn: conn,
	}
	settings := ps.DefaultReceiveSettings

	receiver, _ := ps.NewReceiver(context.Background(), config, settings)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	receivedMessages := make(chan broker.Message, 10)
	errc := make(chan error, 1)

	go func() {
		defer close(receivedMessages)
		defer close(errc)

		if err := receiver.Receive(ctx, func(ctx context.Context, message broker.Message) {
			receivedMessages <- message
		}); err != nil {
			errc <- err
		}
	}()

	results, errs := collectBrokerMessagesAndErrors(receivedMessages, errc)

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

func collectBrokerMessagesAndErrors(receivedMessages chan broker.Message, errc chan error) ([]broker.Message, []error) {
	resultsDrain := make(chan []broker.Message)

	go func() {
		defer close(resultsDrain)

		var results []broker.Message

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
