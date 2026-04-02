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

// Iterator models a simple Kafka consumer, which yields a single message at a time.
type Iterator struct {
	client *kgo.Client
}

// ConsumerConfig defines the configuration properties needed for initializing a Kafka consumer.
//
// All fields of the struct need to be set in order to successfully initialize the consumer.
type ConsumerConfig struct {
	// BrokerAddr a comma-separated list of at least one broker which is a member of the target cluster.
	//
	// After establishing the initial connection, the listed broker(s) will provide the
	// consumer with information about the remaining brokers of the cluster.
	//
	// It's recommended that this list contains as many members of the target Kafka cluster as possible,
	// to ensure the initial connection doesn't fail due to the listed broker(s) being temporarily unavailable.
	BrokerAddr string

	// GroupID the name of the consumer group of this consumer.
	GroupID string

	// Topic the target topic of this consumer.
	//
	// Multiple topics can be provided by separating the topic names with a comma.
	Topic string

	// TLS the tls configuration.
	//
	// If nil, the consumer will not use tls.
	TLS *tls.Config

	// InsecureSkipVerify controls whether a client verifies the server's
	// certificate chain and host name. If InsecureSkipVerify is true, crypto/tls
	// accepts any certificate presented by the server and any host name in that
	// certificate. In this mode, TLS is susceptible to machine-in-the-middle
	// attacks unless custom verification is used. This should be used only for testing.
	InsecureSkipVerify bool

	// Kerberos the SASL Kerberos configuration.
	//
	// If nil, the consumer will not use Kerberos.
	Kerberos *KerberosConfig

	// PlainSASL the SASL/PLAIN configuration.
	//
	// If nil, the consumer will not use SASL/PLAIN.
	PlainSASL *PlainSASLConfig

	// Prometheus the Prometheus configuration.
	//
	// If nil, the consumer will not use Prometheus metrics.
	Prometheus *PrometheusConfig
}

// ConsumerSettings the optional settings for a Kafka consumer.
//
// These settings are fully optional and are preset to sane defaults.
type ConsumerSettings struct {
	// MinBytes the minimum amount of data that a consumer wants to receive from the broker when fetching records.
	// If a broker receives a request for records from a consumer but the new records amount to fewer bytes than MinBytes,
	// the broker will wait until more messages are available before sending the records back to the consumer, reducing
	// the load on both the consumer and the broker, as they have to handle fewer back-and-forth messages
	// in cases where the topics don’t have much new activity.
	//
	// Keep in mind that increasing this value can increase latency for low-throughput cases.
	//
	// This is equivalent to the fetch.min.bytes setting of the Java client.
	MinBytes int

	// MaxWait defines how long will Kafka wait for at least MinBytes-worth of new records to appear, before
	// sending them to the consumer.
	//
	// If you set MaxWait to 100 ms and MinBytes to 1 MB, Kafka will receive a fetch request from the consumer and
	// will respond with data either when it has 1 MB of data to return or after 100 ms, whichever happens first.
	//
	// This is equivalent to the fetch.max.wait.ms setting of the Java client.
	MaxWait time.Duration

	// MaxBytes the maximum amount of bytes Kafka will return whenever the consumer polls a broker.
	// It is used to limit the size of memory that the consumer will use to store data that was returned from the server,
	// irrespective of how many partitions or messages were returned.
	//
	// This is equivalent to the fetch.max.bytes setting of the Java client.
	MaxBytes int

	// MaxConcurrentFetches sets the maximum number of fetch requests to allow in flight or buffered at once,
	// overriding the unbounded (i.e. number of brokers) default.
	//
	// This setting, paired with MaxBytes, can upper bound the maximum amount of memory that the client can use for consuming.
	// Requests are issued to brokers in a FIFO order: once the client is ready to issue a request to a broker,
	// it registers that request and issues it in order with other registrations.
	//
	// A value of 0 implies the allowed concurrency is unbounded and will be limited only by the number of brokers in the cluster.
	MaxConcurrentFetches int

	// Transactional determines if the consumer consumes messages that a transactional producer could be committing
	// which enables to have exactly-once delivery guarantee.
	Transactional bool
}

// DefaultConsumerSettings stores the default values for ConsumerSettings.
var DefaultConsumerSettings = ConsumerSettings{
	MinBytes:             100,
	MaxWait:              5 * time.Second,
	MaxBytes:             10 * 1024 * 1024,
	MaxConcurrentFetches: 3,
}

// NewIterator returns a new instance of Iterator, configured from the provided ConsumerConfig and ConsumerSettings.
func NewIterator(ctx context.Context, config ConsumerConfig, settings ConsumerSettings) (*Iterator, error) {
	client, err := configureIteratorClient(ctx, config, settings)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		client: client,
	}, nil
}

// configureIteratorOptions is a helper function for setting Kafka options for
// the client from ConsumerConfig and ConsumerSettings.
func configureIteratorOptions(config ConsumerConfig, settings ConsumerSettings) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(splitByComma(config.BrokerAddr)...),
		kgo.MaxConcurrentFetches(settings.MaxConcurrentFetches),
		kgo.FetchMinBytes(int32(settings.MinBytes)),
		kgo.FetchMaxWait(settings.MaxWait),
		kgo.FetchMaxBytes(int32(settings.MaxBytes)),
		kgo.FetchMaxPartitionBytes(int32(settings.MaxBytes)),
		kgo.BrokerMaxReadBytes(int32(2 * settings.MaxBytes)),
		kgo.ConsumeTopics(splitByComma(config.Topic)...),
		kgo.ConsumerGroup(config.GroupID),
		kgo.DisableAutoCommit(),
	}

	if settings.Transactional {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.RequireStableFetchOffsets())
	}

	if config.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(config.TLS))
	} else {
		if config.PlainSASL != nil {
			// TLS has to be set when using SASL.
			// If it's not set, we set it here.
			opts = append(
				opts,
				kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: config.InsecureSkipVerify}), //nolint:gosec //need insecure TLS option for testing
			)
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

	return opts, nil
}

// configureIteratorClient configures the github.com/twmb/franz-go client, pinging it after initialization to ensure
// the connection is actually made successfully.
func configureIteratorClient(ctx context.Context, config ConsumerConfig, settings ConsumerSettings) (*kgo.Client, error) {
	opts, _ := configureIteratorOptions(config, settings)

	client, err := kgo.NewClient(opts...) //nolint:contextcheck //cannot pass the context
	if err != nil {
		return nil, err
	}
	// Since client.Ping(ctx) currently (kgo v1.10) doesn't use security authorization it times out
	// when a security protocol (SASL/Plain or Kerberos) is used and that's why we added a check condition.
	if config.Kerberos == nil && config.PlainSASL == nil {
		if err = client.Ping(ctx); err != nil {
			return nil, err
		}
	}

	return client, nil
}

func splitByComma(src string) []string {
	split := strings.Split(src, ",")
	for i, s := range split {
		split[i] = strings.TrimSpace(s)
	}

	return split
}

// NextRecord yields the next Kafka record.
func (it *Iterator) NextRecord(ctx context.Context) (broker.Record, error) {
	fetches := it.client.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		return broker.Record{}, err
	}

	return intoBrokerRecord(ctx, fetches.Records()[0], it.client), nil
}

// intoBrokerRecord maps an instance of github.com/twmb/franz-go record to broker.Record.
func intoBrokerRecord(_ context.Context, record *kgo.Record, client *kgo.Client) broker.Record {
	return broker.Record{
		Offset:        record.Offset,
		Partition:     int64(record.Partition),
		Key:           string(record.Key),
		Value:         record.Value,
		Attributes:    headersIntoAttributes(record.Headers),
		PublishTime:   record.Timestamp,
		IngestionTime: time.Now(),
		CommitFunc: func() {
			_ = client.CommitRecords(context.Background(), record)
		},
	}
}

// headersIntoAttributes maps a slice of key value pairs to a map[string]interface{}.
func headersIntoAttributes(headers []kgo.RecordHeader) map[string]interface{} {
	var attributes map[string]interface{}
	if len(headers) > 0 {
		attributes = make(map[string]interface{}, len(headers))
		for _, header := range headers {
			attributes[header.Key] = string(header.Value)
		}
	}

	return attributes
}

func (it *Iterator) Batched(maxPollRecords int) *BatchIterator {
	return &BatchIterator{
		client:         it.client,
		maxPollRecords: maxPollRecords,
	}
}

// Close closes the connection to the Kafka cluster.
func (it *Iterator) Close() {
	it.client.Close()
}

// BatchIterator models a Kafka consumer that iterates over batches of records.
type BatchIterator struct {
	client         *kgo.Client
	maxPollRecords int
}

// BatchConsumerSettings the optional settings for a batched Kafka consumer.
//
// These settings are fully optional and are preset to sane defaults.
type BatchConsumerSettings struct {
	ConsumerSettings

	// MaxPollRecords the maximum number of records that a single call to poll()
	// will return. Use this to control the amount of data (but not the size of data) your
	// application will need to process in one iteration.
	//
	// Keep in mind that this is only the maximum number of records; there's no guarantee
	// the BatchIterator will return MaxPollRecords even if the state of the topic the iterator
	// consumes from allows it.
	MaxPollRecords int
}

// DefaultBatchConsumerSettings stores the default values for BatchConsumerSettings.
var DefaultBatchConsumerSettings = BatchConsumerSettings{
	ConsumerSettings: DefaultConsumerSettings,
	MaxPollRecords:   100,
}

// NewBatchIterator returns a new instance of BatchIterator, configured from the provided ConsumerConfig and ConsumerSettings.
func NewBatchIterator(ctx context.Context, config ConsumerConfig, settings BatchConsumerSettings) (*BatchIterator, error) {
	client, err := configureIteratorClient(ctx, config, settings.ConsumerSettings)
	if err != nil {
		return nil, errors.Wrap(err, "iterator initialization failed")
	}

	return &BatchIterator{
		client:         client,
		maxPollRecords: settings.MaxPollRecords,
	}, nil
}

// NextBatch yields the next batch of records from Kafka.
func (it *BatchIterator) NextBatch(ctx context.Context) ([]broker.Record, error) {
	fetches := it.client.PollRecords(ctx, it.maxPollRecords)
	if err := fetches.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return it.drainFetches(ctx, fetches), nil
		}

		return nil, err
	}

	return it.drainFetches(ctx, fetches), nil
}

// drainFetches maps the fetch responses received from the broker to an instance of broker.Batch.
func (it *BatchIterator) drainFetches(ctx context.Context, fetches kgo.Fetches) []broker.Record {
	records := fetches.Records()
	parsed := make([]broker.Record, len(records))

	for i, record := range records {
		parsed[i] = intoBrokerRecord(ctx, record, it.client)
	}

	return parsed
}

// Close closes the connection to the Kafka cluster.
func (it *BatchIterator) Close() {
	it.client.Close()
}
