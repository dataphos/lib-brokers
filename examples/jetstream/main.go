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
	"github.com/dataphos/lib-brokers/pkg/broker/jetstream"
	"github.com/dataphos/lib-brokers/pkg/brokerutil"
)

type workMode int

const (
	direct workMode = iota
	streamified
	batched
	batchedStreamified
	asReceiver
)

var mode = batched

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

		<-c

		cancel()
	}()

	switch mode {
	case direct:
		useIterator(ctx)
	case streamified:
		useStreamified(ctx)
	case batched:
		useBatched(ctx)
	case batchedStreamified:
		useBatchedStreamified(ctx)
	case asReceiver:
		useAsReceiver(ctx)
	}
}

func useIterator(ctx context.Context) {
	iterator, err := jetstream.NewIterator(
		jetstream.IteratorConfig{
			URL:          "localhost:4222",
			Subject:      "jetstream.subject.example",
			ConsumerName: "jetstream.consumer.example",
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer iterator.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		message, err := iterator.NextMessage(ctx)
		if err != nil {
			log.Println(err)
			return
		}
		// do something here
		_ = message
		message.Ack()
	}
}

func useStreamified(ctx context.Context) {
	iterator, err := jetstream.NewIterator(
		jetstream.IteratorConfig{
			URL:          "localhost:4222",
			Subject:      "jetstream.subject.example",
			ConsumerName: "jetstream.consumer.example",
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer iterator.Close()

	stream := brokerutil.StreamifyMessageIterator(ctx, iterator)
	for result := range stream {
		if result.Err != nil {
			log.Println(result.Err)
			return
		}
		message := result.Message
		// do something here
		message.Ack()
	}
}

func useBatched(ctx context.Context) {
	iterator, err := jetstream.NewBatchIterator(
		ctx,
		jetstream.IteratorConfig{
			URL:          "localhost:4222",
			Subject:      "jetstream.subject.example",
			ConsumerName: "jetstream.consumer.example",
		},
		jetstream.DefaultBatchIteratorSettings,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer iterator.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		messages, err := iterator.NextBatch(ctx)
		if err != nil {
			log.Println(err)
			return
		}
		for _, message := range messages {
			// do something here
			_ = message
			message.Ack()
		}

	}
}

func useBatchedStreamified(ctx context.Context) {
	iterator, err := jetstream.NewBatchIterator(
		ctx,
		jetstream.IteratorConfig{
			URL:          "localhost:4222",
			Subject:      "jetstream.subject.example",
			ConsumerName: "jetstream.consumer.example",
		},
		jetstream.DefaultBatchIteratorSettings,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer iterator.Close()

	stream := brokerutil.StreamifyMessageBatchIterator(
		ctx,
		iterator,
	)
	for result := range stream {
		if result.Err != nil {
			log.Println(result.Err)
			return
		}
		for _, message := range result.Messages {
			// do something here
			_ = message
			message.Ack()
		}
	}
}

func useAsReceiver(ctx context.Context) {
	iterator, err := jetstream.NewBatchIterator(
		ctx,
		jetstream.IteratorConfig{
			URL:          "localhost:4222",
			Subject:      "jetstream.subject.example",
			ConsumerName: "jetstream.consumer.example",
		},
		jetstream.DefaultBatchIteratorSettings,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer iterator.Close()

	receiver := brokerutil.BatchedMessageIteratorIntoReceiver(
		iterator,
		brokerutil.IntoReceiverSettings{
			NumGoroutines: 10,
		},
	)

	if err := receiver.Receive(ctx, func(ctx context.Context, message broker.Message) {
		// do something here

		message.Ack()
	}); err != nil {
		log.Println(err)
	}
}
