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

package gcs_test

import (
	"context"
	"io"
	"strconv"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/iterator"
	
	"github.com/dataphos/lib-brokers/pkg/broker"
	gcs "github.com/dataphos/lib-brokers/pkg/broker/gcs"
)

func TestNewPublisher(t *testing.T) {
	t.Parallel()
	server, client := setupFakeServer(t, "test-bucket","test-bucket-2")
	defer server.Stop()
	t.Log(listBuckets(t,client,"new-client-test1"))
	settings := gcs.DefaultPublishSettings
	config := gcs.PublisherConfig{
		ProjectID:     "new-client-test",
		StorageClient: client,
	}

	publisher, err := gcs.NewPublisher(context.Background(), config, settings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}

	err = publisher.Close()
	if err != nil {
		t.Fatal("Error while closing publisher: ", err)
	}
}

func TestPublishOneMessage(t *testing.T) {
	t.Parallel()

	server, client := setupFakeServer(t, "publish-test-bucket")
	defer server.Stop()

	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:     "publish-one-message-test",
		StorageClient: client,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	topic, err := publisher.Topic("publish-test-bucket")
	if err != nil {
		t.Fatal("Error while referencing bucket: ", err)
	}

	message := broker.OutboundMessage{
		Key:        "key1",
		Data:       []byte("Simple message."),
		Attributes: nil,
	}

	err = topic.Publish(context.Background(), message)
	if err != nil {
		t.Fatal("Error while publishing message: ", err)
	}

	objects := listBucketObjects(t, client, "publish-test-bucket")
	t.Log("Objects: ", objects)
	if len(objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objects))
	}

	data := readObject(t, client, "publish-test-bucket", objects[0])
	t.Log("Data: ", string(data))
	if string(data) != string(message.Data) {
		t.Fatalf("expected %s, got %s", message.Data, data)
	}
}

func TestPublishMoreMessages(t *testing.T) {
	t.Parallel()

	server, client := setupFakeServer(t, "publish-more-messages-bucket")
	defer server.Stop()

	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:     "publish-more-messages-test",
		StorageClient: client,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	topic, err := publisher.Topic("publish-more-messages-bucket")
	if err != nil {
		t.Fatal("Error while referencing bucket: ", err)
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

	for _, message := range messages {
		err = topic.Publish(context.Background(), message)
		if err != nil {
			t.Fatal("Error while publishing message: ", err)
		}
	}

	objects := listBucketObjects(t, client, "publish-more-messages-bucket")
	if len(objects) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(objects))
	}

	payloads := collectObjectPayloads(t, client, "publish-more-messages-bucket", objects)

	for _, message := range messages {
		if _, found := payloads[string(message.Data)]; !found {
			t.Fatalf("Message %s not found in bucket", string(message.Data))
		}
	}
}

func TestPublishBatch(t *testing.T) {
	t.Parallel()

	server, client := setupFakeServer(t, "batch-publish-bucket")
	defer server.Stop()

	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:     "publish-batch-test",
		StorageClient: client,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	topic, err := publisher.Topic("batch-publish-bucket")
	if err != nil {
		t.Fatal("Error while referencing bucket: ", err)
	}

	numMsgs := 50
	messages := make([]broker.OutboundMessage, 0, numMsgs)

	for i := 0; i < numMsgs; i++ {
		messages = append(messages, broker.OutboundMessage{
			Key:        "key",
			Data:       []byte(strconv.Itoa(i)),
			Attributes: nil,
		})
	}

	err = topic.BatchPublish(context.Background(), messages...)
	if err != nil {
		t.Fatal("Error while batch publishing: ", err)
	}

	objects := listBucketObjects(t, client, "batch-publish-bucket")
	if len(objects) != numMsgs {
		t.Fatalf("expected %d objects, got %d", numMsgs, len(objects))
	}

	payloads := collectObjectPayloads(t, client, "batch-publish-bucket", objects)

	for _, message := range messages {
		dataString := string(message.Data)

		if _, found := payloads[dataString]; !found {
			t.Fatalf("Message %s not found in bucket", dataString)
		}
	}
}

func listBuckets(t *testing.T, client *storage.Client, projectID string) []string {
	t.Helper()

	var names []string

	it := client.Buckets(context.Background(), projectID)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			t.Fatal("Error listing buckets: ", err)
		}

		names = append(names, attrs.Name)
	}

	return names
}

func listBucketObjects(t *testing.T, client *storage.Client, bucket string) []string {
	t.Helper()

	var names []string

	it := client.Bucket(bucket).Objects(context.Background(), nil)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			t.Fatal("Error listing objects: ", err)
		}

		names = append(names, attrs.Name)
	}

	return names
}

func readObject(t *testing.T, client *storage.Client, bucket, object string) []byte {
	t.Helper()

	reader, err := client.Bucket(bucket).Object(object).NewReader(context.Background())
	if err != nil {
		t.Fatal("Error creating object reader: ", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal("Error reading object data: ", err)
	}

	return data
}

func readObjectAttrs(t *testing.T, client *storage.Client, bucket, object string) *storage.ObjectAttrs {
	t.Helper()

	attrs, err := client.Bucket(bucket).Object(object).Attrs(context.Background())
	if err != nil {
		t.Fatal("Error reading object attributes: ", err)
	}

	return attrs
}

func collectObjectPayloads(t *testing.T, client *storage.Client, bucket string, objects []string) map[string]struct{} {
	t.Helper()

	payloads := make(map[string]struct{}, len(objects))

	for _, obj := range objects {
		data := readObject(t, client, bucket, obj)
		payloads[string(data)] = struct{}{}
	}

	return payloads
}

func setupFakeServer(t *testing.T, buckets ...string) (*fakestorage.Server, *storage.Client) {
	t.Helper()

	server := fakestorage.NewServer([]fakestorage.Object{})

	for _, b := range buckets {
		server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: b})
	}

	return server, server.Client()
}
