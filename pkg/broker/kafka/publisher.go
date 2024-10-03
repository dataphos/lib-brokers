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

package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	krbconf "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// Publisher models a Kafka producer.
type Publisher struct {
	client *kgo.Client
}

// ProducerConfig defines the configuration properties needed for initializing a Kafka producer.
//
// All fields of the struct need to be set in order to successfully initialize the producer.
type ProducerConfig struct {
	// BrokerAddr a list of at least one broker which is a member of the target cluster.
	//
	// After establishing the initial connection, the listed broker(s) will provide the
	// producer with information about the remaining brokers of the cluster.
	//
	// It's recommended that this list contains as many members of the target Kafka cluster as possible,
	// to ensure the initial connection doesn't fail due to the listed broker(s) being temporarily unavailable.
	BrokerAddr string

	// TLS the tls configuration.
	//
	// If nil, the producer will not use tls.
	TLS *tls.Config

	// InsecureSkipVerify controls whether a client verifies the server's
	// certificate chain and host name. If InsecureSkipVerify is true, crypto/tls
	// accepts any certificate presented by the server and any host name in that
	// certificate. In this mode, TLS is susceptible to machine-in-the-middle
	// attacks unless custom verification is used. This should be used only for testing.
	InsecureSkipVerify bool

	// Kerberos the SASL Kerberos configuration.
	//
	// If nil, the producer will not use Kerberos.
	Kerberos *KerberosConfig

	// PlainSASL the SASL/PLAIN configuration.
	//
	// If nil, the producer will not use SASL/PLAIN.
	PlainSASL *PlainSASLConfig

	// Prometheus the Prometheus configuration.
	//
	// If nil, the producer will not use Prometheus metrics.
	Prometheus *PrometheusConfig

	// DisableCompression flag that specifies if message compression is disabled.
	//
	// If true, compression is disabled.
	DisableCompression bool

	// TransactionalID used as a transactional ID if the producer is set to be a part of
	// a transaction.
	TransactionalID string
}

// ProducerSettings the optional settings for a Kafka producer.
//
// These settings are fully optional and are preset to sane defaults.
type ProducerSettings struct {
	// BatchSize sets the max amount of records the client will buffer,
	// blocking new produces until records are finished if this limit is reached.
	BatchSize int

	// BatchBytes when multiple records are sent to the same partition, the producer will batch them
	// together. BatchBytes parameter controls the amount of memory in bytes that will be used for each batch.
	//
	// BatchBytes does not mean that the producer will wait for the batch to
	// become full. The producer will send half-full batches and even batches with just a single message in them.
	// Therefore, setting the batch size too large will not cause delays in sending messages; it will just use more memory for the batches.
	//
	// This is equivalent to the batch.size setting of the Java client.
	BatchBytes int64

	// Linger controls the amount of time to wait for additional messages before sending the current batch.
	// The producer sends a batch of messages either when the current batch is full or when the Linger limit is reached,
	// whatever comes first.
	//
	// Linger is specific to a topic partition.
	// A high volume producer will likely be producing to many partitions;
	// it is both unnecessary to linger in this case and inefficient because the client will have many timers running
	// (and stopping and restarting) unnecessarily.
	//
	// This is equivalent to the linger.ms setting of the Java client.
	Linger time.Duration

	// Acks represents the number of acks a broker leader must have before
	// a produce request is considered complete.
	//
	// This controls the durability of written records and corresponds to "acks" in
	// Kafka's Producer Configuration documentation. Allowed values are LeaderAck, AllISRAcks, NoAck and RequiredAcks.
	// Setting Acks to a value other than AllISRAcks implicitly disables the producer's idempotency option.
	//
	// The default is LeaderAck.
	Acks kgo.Acks
}

// DefaultProducerSettings stores the default values for ProducerSettings.
var DefaultProducerSettings = ProducerSettings{
	BatchSize:  40,
	BatchBytes: 5 * 1024 * 1024,
	Linger:     10 * time.Millisecond,
	Acks:       kgo.AllISRAcks(),
}

var defaultCompressionCodecs = []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.ZstdCompression(), kgo.GzipCompression(), kgo.Lz4Compression()}

// NewPublisher returns a new instance of Publisher, configured from the provided ProducerConfig and ProducerSettings.
func NewPublisher(ctx context.Context, config ProducerConfig, settings ProducerSettings) (*Publisher, error) {
	client, err := configurePublisherClient(ctx, config, settings)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		client: client,
	}, nil
}

// configurePublisherOptions is a helper function for setting Kafka options for
// the client from ProducerConfig and ProducerSettings.
func configurePublisherOptions(config ProducerConfig, settings ProducerSettings) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(config.BrokerAddr, ",")...),
		kgo.MaxBufferedRecords(settings.BatchSize),
		kgo.ProducerBatchMaxBytes(int32(settings.BatchBytes)),
		kgo.ProducerLinger(settings.Linger),
		kgo.StopProducerOnDataLossDetected(),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(settings.Acks),
	}

	if config.DisableCompression {
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	} else {
		opts = append(opts, kgo.ProducerBatchCompression(defaultCompressionCodecs...))
	}

	if settings.Acks != kgo.AllISRAcks() {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	if config.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(config.TLS))
	} else {
		if config.PlainSASL != nil {
			// TLS has to be set when using SASL.
			// If it's not already set by the user, we set it here, since using SASL without TLS is insecure, as without it,
			// credentials are passed in plaintext across the network.
			opts = append(
				opts,
				kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: config.InsecureSkipVerify})) //nolint:gosec //need insecure TLS option for testing
		}
	}

	if config.Kerberos != nil && config.PlainSASL != nil {
		return nil, errors.New("Can not use multiple SASL authentication mechanisms simultaneously")
	}

	if config.Kerberos != nil {
		krb5keytab, err := keytab.Load(config.Kerberos.KeyTabPath)
		if err != nil {
			return nil, errors.Wrap(err, "error loading kerberos keytab file")
		}

		krb5conf, err := krbconf.Load(config.Kerberos.ConfigPath)
		if err != nil {
			return nil, errors.Wrap(err, "error loading kerberos config file")
		}

		opts = configureKerberos(opts, krb5conf, krb5keytab, config.Kerberos.Username, config.Kerberos.Realm, config.Kerberos.Service)
	}

	if config.PlainSASL != nil {
		opts = append(opts, kgo.SASL(plain.Auth{
			Zid:  config.PlainSASL.Zid,
			User: config.PlainSASL.User,
			Pass: config.PlainSASL.Pass,
		}.AsMechanism()))
	}

	if config.Prometheus != nil {
		opts = append(opts, kgo.WithHooks(kprom.NewMetrics(
			config.Prometheus.Namespace,
			kprom.Registerer(config.Prometheus.Registerer),
			kprom.Gatherer(config.Prometheus.Gatherer),
		)))
	}

	if config.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(config.TransactionalID))
	}

	return opts, nil
}

// configurePublisherClient configures the github.com/twmb/franz-go client, pinging it after initialization to ensure
// the connection is actually made successfully.
func configurePublisherClient(ctx context.Context, config ProducerConfig, settings ProducerSettings) (*kgo.Client, error) {
	opts, err := configurePublisherOptions(config, settings)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...) //nolint:contextcheck //cannot pass the context
	if err != nil {
		return nil, err
	}
	// Since client.Ping(ctx) currently (kgo v1.10) doesn't use security authorization it times out
	// when a security protocol (SASL/Plain) is used and that's why we added a check condition.
	if config.PlainSASL == nil {
		if err = client.Ping(ctx); err != nil {
			return nil, err
		}
	}

	return client, nil
}

// Topic yields a new instance of broker.Topic.
//
// This is a lightweight abstraction; calling this for every record is acceptable in terms of additional overhead.
func (p *Publisher) Topic(topicID string) (broker.Topic, error) {
	return &Topic{
		client:  p.client,
		topicID: topicID,
	}, nil
}

// Close closes the connection to the Kafka cluster.
func (p *Publisher) Close() error {
	p.client.Close()

	return nil
}

// Topic models a Kafka producer with a set, "hardcoded" destination topic.
type Topic struct {
	client  *kgo.Client
	topicID string
}

// Publish publishes a Kafka record to this topic.
//
// Blocks until the record is fully committed to the Kafka cluster.
func (t *Topic) Publish(ctx context.Context, message broker.OutboundMessage) error {
	return t.client.ProduceSync(ctx, t.intoKafkaRecord(message)).FirstErr()
}

// BatchPublish publishes a batch of Kafka records to this topic.
//
// Blocks until the records are fully committed to the Kafka cluster.
// May fail if ProducerBatchMaxBytes is set to a value higher than the broker accepts.
func (t *Topic) BatchPublish(ctx context.Context, messages ...broker.OutboundMessage) error {
	records := make([]*kgo.Record, 0, len(messages))

	for _, message := range messages {
		records = append(records, t.intoKafkaRecord(message))
	}

	return t.client.ProduceSync(ctx, records...).FirstErr()
}

// intoKafkaRecord maps the given broker.OutboundMessage to an instance of github.com/twmb/franz-go record.
func (t *Topic) intoKafkaRecord(message broker.OutboundMessage) *kgo.Record {
	headers, key := getKeyAndHeaders(message)

	return &kgo.Record{
		Topic:   t.topicID,
		Key:     key,
		Headers: headers,
		Value:   message.Data,
	}
}

// getKeyAndHeaders is a helper function for extracting message attributes and the key.
func getKeyAndHeaders(message broker.OutboundMessage) ([]kgo.RecordHeader, []byte) {
	var headers []kgo.RecordHeader
	if message.Attributes != nil {
		headers = make([]kgo.RecordHeader, 0, len(message.Attributes))

		for key, value := range message.Attributes {
			// check that the value of the attribute is not nil.
			if value == nil {
				panic(fmt.Sprintf("Value of attribute %q is nil. An attribute'stringValue value should not be nil.", key))
			}

			stringValue, ok := value.(string)
			if !ok {
				stringValue = fmt.Sprintf("%s", value)
			}

			headers = append(headers, kgo.RecordHeader{
				Key:   key,
				Value: []byte(stringValue),
			})
		}
	}

	var key []byte
	if message.Key != "" {
		key = []byte(message.Key)
	}

	return headers, key
}
