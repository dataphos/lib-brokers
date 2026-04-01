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

package gcs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/option"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// Publisher models a Google Cloud Storage writer that implements the broker.Publisher interface.
// Instead of publishing to a messaging topic, messages are written as objects to a GCS bucket.
type Publisher struct {
	client               *storage.Client
	settings             PublishSettings
	skipBucketHealthCheck bool
}

// PublisherConfig defines configuration properties needed for initializing a GCS publisher.
type PublisherConfig struct {
	// ProjectID is the unique identifier of the GCP project.
	ProjectID string

	// CredentialsFile optionally sets the path to a service account JSON key file.
	// If empty, Application Default Credentials are used.
	CredentialsFile string

	// Endpoint optionally overrides the default GCS endpoint (useful for emulators/testing).
	Endpoint string

	// StorageClient optionally provides a pre-configured *storage.Client.
	// When set, CredentialsFile and Endpoint are ignored.
	// Useful for testing with fake GCS servers.
	StorageClient *storage.Client
}

// PublishSettings defines optional settings for the GCS publisher.
type PublishSettings struct {
	// ObjectPrefix is prepended to every generated object name.
	ObjectPrefix string

	// ContentType is the MIME type set on uploaded objects.
	ContentType string

	// ObjectNamer optionally provides a custom function to generate object names.
	// It receives the message and must return a unique object name.
	// If nil, a default naming strategy based on timestamp and random suffix is used.
	ObjectNamer func(broker.OutboundMessage) string
}

// DefaultPublishSettings stores the default values for PublishSettings.
var DefaultPublishSettings = PublishSettings{
	ObjectPrefix: "",
	ContentType:  "application/octet-stream",
	ObjectNamer:  nil,
}

// NewPublisher returns a new instance of Publisher, configured from the provided PublisherConfig and PublishSettings.
func NewPublisher(ctx context.Context, config PublisherConfig, settings PublishSettings) (*Publisher, error) {
	if config.StorageClient != nil {
		return &Publisher{
			client:               config.StorageClient,
			settings:             settings,
			skipBucketHealthCheck: true,
		}, nil
	}

	var opts []option.ClientOption

	if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	if config.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(config.Endpoint), option.WithoutAuthentication())
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GCS client")
	}

	return &Publisher{
		client:   client,
		settings: settings,
	}, nil
}

// Topic returns a new broker.Topic backed by the given GCS bucket.
// The bucketName parameter is used as the destination bucket, analogous to a topic ID in messaging brokers.
func (p *Publisher) Topic(bucketName string) (broker.Topic, error) {
	bucket := p.client.Bucket(bucketName)

	if !p.skipBucketHealthCheck {
		if err := doBucketHealthCheck(context.Background(), bucket); err != nil {
			return nil, err
		}
	}

	return &Topic{
		bucket:   bucket,
		settings: p.settings,
	}, nil
}

// Close closes the underlying GCS client connection.
func (p *Publisher) Close() error {
	return p.client.Close()
}

// Topic models a GCS bucket destination that implements the broker.Topic interface.
type Topic struct {
	bucket   *storage.BucketHandle
	settings PublishSettings
}

// Publish writes a single message as a GCS object.
// The object name is generated using the configured ObjectNamer or a default timestamp-based strategy.
// Message attributes are stored as object metadata.
func (t *Topic) Publish(ctx context.Context, message broker.OutboundMessage) error {
	objectName := t.objectName(message)

	writer := t.bucket.Object(objectName).NewWriter(ctx)
	writer.ContentType = t.settings.ContentType

	if message.Attributes != nil {
		metadata := make(map[string]string, len(message.Attributes))

		for key, value := range message.Attributes {
			if v, ok := value.(string); ok {
				metadata[key] = v

				continue
			}

			metadata[key] = fmt.Sprintf("%s", value)
		}

		writer.Metadata = metadata
	}

	if _, err := writer.Write(message.Data); err != nil {
		_ = writer.Close()

		return errors.Wrap(err, "failed to write object data")
	}

	if err := writer.Close(); err != nil {
		return errors.Wrap(err, "failed to finalize object upload")
	}

	return nil
}

// BatchPublish writes multiple messages to the GCS bucket in parallel.
func (t *Topic) BatchPublish(ctx context.Context, messages ...broker.OutboundMessage) error {
	return broker.SimplePublisherParallelization(ctx, t, messages...)
}

func (t *Topic) objectName(message broker.OutboundMessage) string {
	if t.settings.ObjectNamer != nil {
		return t.settings.ObjectNamer(message)
	}

	suffix := randomHex(8)
	ts := time.Now().UTC().Format("2006/01/02/150405")

	var key string
	if message.Key != "" {
		key = message.Key + "_"
	}

	return fmt.Sprintf("%s%s/%s%s", t.settings.ObjectPrefix, ts, key, suffix)
}

func randomHex(n int) string {
	b := make([]byte, n)

	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}
