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
	"strings"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dataphos/lib-brokers/pkg/broker"
)

// Transaction models a Kafka transaction session.
//
// This model provides exactly-once delivery of the produced messages. More
// specifically, the messages consumed over this Transaction struct, processed in
// any way by the client application and published using the TransactionalTopic
// that contains this Transaction as it's element will be delivered exactly once.
// Therefore, a potential consumer consuming those messages should be configured
// to be transactional (setting ConsumerSettings.Transactional to true).
//
// To start producing in a transaction, the method Transaction.Begin should be
// called first. After producing, the transaction should be either committed
// or aborted (Transaction.Commit, Transaction.Abort). Multiple
// begin - commit/abort transaction sessions can be held on this Transaction,
// meaning that there is no need to create a new instance for every desired
// transactional session. Before shutting down, Transaction.Close should be called.
type Transaction struct {
	session        *kgo.GroupTransactSession
	maxPollRecords int
	iterator       *BatchIterator
}

// NewTransaction returns a new instance of Transaction, configured from the
// provided ProducerConfig, ProducerSettings, ConsumerConfig and BatchConsumerSettings.
func NewTransaction(ctx context.Context,
	producerConfig ProducerConfig, producerSettings ProducerSettings,
	consumerConfig ConsumerConfig, consumerSettings BatchConsumerSettings,
) (*Transaction, error) {
	if strings.Compare(consumerConfig.BrokerAddr, producerConfig.BrokerAddr) != 0 {
		return nil, errors.New("consumer and producer broker addresses do not match")
	}

	session, err := configureTransactionSession(ctx, producerConfig, producerSettings, consumerConfig, consumerSettings)
	if err != nil {
		return nil, err
	}

	batchIterator, err := NewBatchIterator(ctx, consumerConfig, consumerSettings)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		session:        session,
		maxPollRecords: consumerSettings.MaxPollRecords,
		iterator:       batchIterator,
	}, nil
}

// configureTransactionSession configures the github.com/twmb/franz-go transaction
// session and the underlying client. The client is pinged after initialization
// to ensure the connection is actually made successfully.
func configureTransactionSession(ctx context.Context, producerConfig ProducerConfig, producerSettings ProducerSettings,
	consumerConfig ConsumerConfig, consumerSettings BatchConsumerSettings,
) (*kgo.GroupTransactSession, error) {
	publisherOpts, err := configurePublisherOptions(producerConfig, producerSettings)
	if err != nil {
		return nil, err
	}

	consumerOpts, err := configureIteratorOptions(consumerConfig, consumerSettings.ConsumerSettings)
	if err != nil {
		return nil, err
	}

	opts := consumerOpts
	opts = append(opts, publisherOpts...)

	session, err := kgo.NewGroupTransactSession(opts...) //nolint:contextcheck //cannot pass the context
	if session.Client().Ping(ctx) != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	// when using kerberos the client.Ping(ctx) will always time out because
	// no kerberos handshake is done, so we added one more condition.
	if consumerConfig.Kerberos == nil || producerConfig.Kerberos == nil {
		if err = session.Client().Ping(ctx); err != nil {
			return nil, err
		}
	}

	return session, nil
}

// Begin is a wrapper around github.com/twmb/franz-go's GroupTransactSession's Begin
// with the exact same semantics.
//
// This function begins a transaction, and returns an error if the client has no
// transactional ID or is already in a transaction. It should be called before
// producing records in a transaction.
func (t *Transaction) Begin() error {
	return t.session.Begin()
}

// Commit is a wrapper around github.com/twmb/franz-go's GroupTransactSession's
// End function, called with an argument demanding to commit the transaction.
func (t *Transaction) Commit(ctx context.Context) (bool, error) {
	return t.session.End(ctx, kgo.TryCommit)
}

// Abort is a wrapper around github.com/twmb/franz-go's GroupTransactSession's
// End function, called with an argument demanding to abort the transaction.
func (t *Transaction) Abort(ctx context.Context) (bool, error) {
	return t.session.End(ctx, kgo.TryAbort)
}

// TransactionalTopic models a transactional Kafka producer with a set,
// "hardcoded" destination topic.
type TransactionalTopic struct {
	session *kgo.GroupTransactSession
	topicID string
}

// Topic yields a new instance of broker.Topic for a Transaction.
//
// This is a lightweight abstraction; calling this for every record is acceptable in terms of additional overhead.
func (t *Transaction) Topic(topicID string) (broker.Topic, error) {
	return &TransactionalTopic{
		session: t.session,
		topicID: topicID,
	}, nil
}

// Publish publishes a Kafka record to this topic.
//
// Blocks until the record is fully committed to the Kafka cluster.
// Before calling this function, the transaction must be initialized by
// calling Transaction.Begin.
func (t *TransactionalTopic) Publish(ctx context.Context, message broker.OutboundMessage) error {
	return t.session.ProduceSync(ctx, t.IntoKafkaRecord(message)).FirstErr()
}

// BatchPublish publishes a batch of Kafka records to this topic.
//
// Blocks until the records are fully committed to the Kafka cluster.
// Before calling this function, the transaction must be initialized by
// calling Transaction.Begin.
func (t *TransactionalTopic) BatchPublish(ctx context.Context, messages ...broker.OutboundMessage) error {
	records := make([]*kgo.Record, 0, len(messages))

	for _, message := range messages {
		records = append(records, t.IntoKafkaRecord(message))
	}

	return t.session.ProduceSync(ctx, records...).FirstErr()
}

// Client returns the underlying Kafka client this Transaction wraps.
func (t *Transaction) Client() *kgo.Client {
	return t.session.Client()
}

// Close is a wrapper around github.com/twmb/franz-go's kgo.GroupTransactSession.Close,
// with the exact same semantics.
//
// This function must be called to leave the group before shutting down.
func (t *Transaction) Close() {
	t.session.Close()
}

// NextBatch yields the next batch of records from Kafka.
func (t *Transaction) NextBatch(ctx context.Context) ([]broker.Record, error) {
	fetches := t.session.PollRecords(ctx, t.maxPollRecords)
	if err := fetches.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return t.iterator.drainFetches(ctx, fetches), nil
		}

		return nil, err
	}

	return t.iterator.drainFetches(ctx, fetches), nil
}

// IntoKafkaRecord maps the given broker.OutboundMessage to an instance of github.com/twmb/franz-go record.
func (t *TransactionalTopic) IntoKafkaRecord(message broker.OutboundMessage) *kgo.Record {
	headers, key := getKeyAndHeaders(message)

	return &kgo.Record{
		Topic:   t.topicID,
		Key:     key,
		Headers: headers,
		Value:   message.Data,
	}
}
