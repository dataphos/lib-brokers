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
	"os"
	"strconv"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/dataphos/lib-brokers/pkg/broker"
	gcs "github.com/dataphos/lib-brokers/pkg/broker/gcs"
)

// Integration tests publish to real GCP buckets.
//
// Required environment variables:
//   GCS_PROJECT_ID      - GCP project ID
//   GCS_VALID_BUCKET    - bucket name for valid messages
//   GCS_INVALID_BUCKET  - bucket name for invalid messages
//
// Optional:
//   GCS_CREDENTIALS_FILE - path to service account JSON key file (uses ADC if not set)
//
// Run with:
//   GCS_PROJECT_ID=my-project GCS_VALID_BUCKET=valid-bkt GCS_INVALID_BUCKET=invalid-bkt go test -v -run Integration ./pkg/broker/gcs/...

type integrationEnv struct {
	projectID       string
	validBucket     string
	invalidBucket   string
	credentialsFile string
}

func loadIntegrationEnv(t *testing.T) integrationEnv {
	t.Helper()

	projectID := os.Getenv("GCS_PROJECT_ID")
	validBucket := os.Getenv("GCS_VALID_BUCKET")
	invalidBucket := os.Getenv("GCS_INVALID_BUCKET")

	if projectID == "" || validBucket == "" || invalidBucket == "" {
		t.Skip("Skipping integration test: set GCS_PROJECT_ID, GCS_VALID_BUCKET, and GCS_INVALID_BUCKET to run")
	}
	t.Log(os.Getenv("GCS_CREDENTIALS_FILE"))
	return integrationEnv{
		projectID:       projectID,
		validBucket:     validBucket,
		invalidBucket:   invalidBucket,
		credentialsFile: os.Getenv("GCS_CREDENTIALS_FILE"),
	}
}

func newVerificationClient(t *testing.T, env integrationEnv) *storage.Client {
	t.Helper()

	var opts []option.ClientOption
	if env.credentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(env.credentialsFile))
	}

	client, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		t.Fatal("Error creating verification client: ", err)
	}
	return client
}

func TestIntegrationNewPublisher(t *testing.T) {
	env := loadIntegrationEnv(t)
	verifyClient := newVerificationClient(t, env)
	defer verifyClient.Close()
	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:       env.projectID,
		CredentialsFile: env.credentialsFile,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}

	err = publisher.Close()
	if err != nil {
		t.Fatal("Error while closing publisher: ", err)
	}
}

func TestIntegrationPublishOneMessage(t *testing.T) {
	env := loadIntegrationEnv(t)
	verifyClient := newVerificationClient(t, env)
	defer verifyClient.Close()

	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:       env.projectID,
		CredentialsFile: env.credentialsFile,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	topic, err := publisher.Topic(env.validBucket)
	if err != nil {
		t.Fatal("Error while referencing bucket: ", err)
	}

	message := broker.OutboundMessage{
		Key:        "integration-key1",
		Data:       []byte("Integration test: simple message."),
		Attributes: nil,
	}

	err = topic.Publish(context.Background(), message)
	if err != nil {
		t.Fatal("Error while publishing message: ", err)
	}

	t.Log("Successfully published one message to ", env.validBucket)
}

func TestIntegrationPublishMoreMessages(t *testing.T) {
	env := loadIntegrationEnv(t)
	verifyClient := newVerificationClient(t, env)
	defer verifyClient.Close()

	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:       env.projectID,
		CredentialsFile: env.credentialsFile,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	topic, err := publisher.Topic(env.validBucket)
	if err != nil {
		t.Fatal("Error while referencing bucket: ", err)
	}

	messages := []broker.OutboundMessage{
		{
			Key:        "integration-key1",
			Data:       []byte("Integration: First message."),
			Attributes: nil,
		},
		{
			Key:        "integration-key2",
			Data:       []byte("Integration: Second message."),
			Attributes: nil,
		},
		{
			Key:        "integration-key3",
			Data:       []byte("Integration: Third message."),
			Attributes: nil,
		},
	}

	for _, message := range messages {
		err = topic.Publish(context.Background(), message)
		if err != nil {
			t.Fatal("Error while publishing message: ", err)
		}
	}

	t.Logf("Successfully published %d messages to %s", len(messages), env.validBucket)
}

func TestIntegrationPublishBatch(t *testing.T) {
	env := loadIntegrationEnv(t)

	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:       env.projectID,
		CredentialsFile: env.credentialsFile,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	topic, err := publisher.Topic(env.validBucket)
	if err != nil {
		t.Fatal("Error while referencing bucket: ", err)
	}

	numMsgs := 50
	messages := make([]broker.OutboundMessage, 0, numMsgs)

	for i := 0; i < numMsgs; i++ {
		messages = append(messages, broker.OutboundMessage{
			Key:        "integration-batch",
			Data:       []byte(strconv.Itoa(i)),
			Attributes: nil,
		})
	}

	err = topic.BatchPublish(context.Background(), messages...)
	if err != nil {
		t.Fatal("Error while batch publishing: ", err)
	}

	t.Logf("Successfully batch published %d messages to %s", numMsgs, env.validBucket)
}

func TestIntegrationPublishToValidAndInvalidBuckets(t *testing.T) {
	env := loadIntegrationEnv(t)

	publisher, err := gcs.NewPublisher(context.Background(), gcs.PublisherConfig{
		ProjectID:       env.projectID,
		CredentialsFile: env.credentialsFile,
	}, gcs.DefaultPublishSettings)
	if err != nil {
		t.Fatal("Error while creating publisher: ", err)
	}
	defer publisher.Close()

	validTopic, err := publisher.Topic(env.validBucket)
	if err != nil {
		t.Fatal("Error while referencing valid bucket: ", err)
	}

	invalidTopic, err := publisher.Topic(env.invalidBucket)
	if err != nil {
		t.Fatal("Error while referencing invalid bucket: ", err)
	}

	validMsg := broker.OutboundMessage{
		Key:  "valid-msg",
		Data: []byte(`{"name":"alice","status":"ok"}`),
		Attributes: map[string]interface{}{
			"validation_status": "valid",
		},
	}

	invalidMsg := broker.OutboundMessage{
		Key:  "invalid-msg",
		Data: []byte(`malformed data`),
		Attributes: map[string]interface{}{
			"validation_status": "invalid",
			"error":             "schema mismatch",
		},
	}

	err = validTopic.Publish(context.Background(), validMsg)
	if err != nil {
		t.Fatal("Error while publishing valid message: ", err)
	}

	err = invalidTopic.Publish(context.Background(), invalidMsg)
	if err != nil {
		t.Fatal("Error while publishing invalid message: ", err)
	}

	t.Logf("Successfully published valid message to %s and invalid message to %s", env.validBucket, env.invalidBucket)
}
