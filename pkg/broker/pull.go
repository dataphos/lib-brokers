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

package broker

import (
	"context"
	"time"
)

// Receiver models an asynchronous, callback-based pulling mechanism of messaging systems which model messages
// as individual objects, meaning that acknowledge operations on a single Message have no side effect on any other message.
type Receiver interface {
	// Receive runs the pulling process, blocking for the entire duration of the pull.
	//
	// Receive executes the provided callback for each pulled Message, meaning it is the responsibility
	// of the Receiver to provide the callbacks with multithreading.
	//
	// Receive blocks until either an error is returned, or when the provided ctx is canceled, in which case, nil is returned.
	Receive(context.Context, func(context.Context, Message)) error
}

// BatchedReceiver models an asynchronous, callback-based pulling mechanism, which, for performance reason
// yields batches of messages at a time, instead of a single message like the Receiver.
//
// This is used by messaging systems which model messages as individual objects,
// meaning that acknowledge operations on a single Message have no side effect on any other message.
type BatchedReceiver interface {
	// ReceiveBatch runs the pulling process, blocking for the entire duration of the pull.
	//
	// ReceiveBatch executes the provided callback for each pulled batch of Message instances,
	// meaning it is the responsibility of the BatchedReceiver to provide the callbacks with multithreading.
	//
	// ReceiveBatch blocks until either an error is returned, or when the provided ctx is canceled, in which case, nil is returned.
	ReceiveBatch(context.Context, func(context.Context, []Message)) error
}

// MessageIterator models an iterative pulling mechanism.
//
// This is used by messaging systems which model messages as individual objects,
// meaning that acknowledge operations on a single Message have no side effect on any other message.
type MessageIterator interface {
	// NextMessage returns the next Message or an error.
	NextMessage(context.Context) (Message, error)
}

// BatchedMessageIterator models an iterative, batch-based pulling mechanism.
//
// This is used by messaging systems which model messages as individual objects,
// meaning that acknowledge operations on a single Message have no side effect on any other message.
//
// Although a default, trivial implementation for any MessageIterator can be easily acquired through an adapter,
// some brokers might have built-in optimizations for pulling batches of messages at a time, meaning it is
// often times more efficient to implement this interface "manually".
type BatchedMessageIterator interface {
	// NextBatch returns the next batch of Message instances.
	NextBatch(ctx context.Context) ([]Message, error)
}

// MessageStream models a concurrent, channel-based pulling mechanism.
//
// This is used by messaging systems which model messages as individual objects,
// meaning that acknowledge operations on a single Message have no side effect on any other message.
type MessageStream interface {
	// Stream starts a message stream.
	//
	// The returned Message channel is closed once the ctx is Done.
	//
	// The returned error channel contains at most one error, depending on if the stream ended prematurely
	// due to some issue with the broker.
	Stream(ctx context.Context) (<-chan Message, <-chan error)
}

// BatchedMessageStream models a concurrent, channel-based pulling mechanism, which, for performance reason
// yields batches of messages at a time, instead of a single message like the MessageStream.
//
// This is used by messaging systems which model messages as individual objects,
// meaning that acknowledge operations on a single Message have no side effect on any other message.
type BatchedMessageStream interface {
	// BatchStream starts a message stream.
	//
	// The returned Message slice channel is closed once the context.Context is canceled.
	//
	// The returned error channel contains at most one error, depending on if the stream ended prematurely
	// due to some issue with the broker.
	BatchStream(ctx context.Context) (<-chan []Message, <-chan error)
}

// Message defines the object retrieved from a messaging system which supports individual message processing.
//
// The fact this struct implies acknowledge operations have no side effect on any other message is implied by
// the existence of NackFunc, as negative acknowledges don't make sense in log-based messaging systems.
type Message struct {
	// ID identifies the message (unique across the source topic).
	ID string

	// Key identifies the payload part of the message.
	// Unlike ID, it doesn't have to be unique across the source topic and is mostly used
	// for ordering guarantees.
	Key string

	// Data holds the payload of the message.
	Data []byte

	// Attributes holds the properties commonly used by brokers to pass metadata.
	Attributes map[string]interface{}

	// PublishTime the time this message was published.
	PublishTime time.Time

	// IngestionTime the time this message was received.
	IngestionTime time.Time

	// AckFunc must be called after successful processing (and not before).
	// This signals to the broker that this Message is safe to delete.
	AckFunc func()

	// NackFunc must be called if the processing has failed.
	// This signals to the broker that this Message is not safe to delete.
	NackFunc func()
}

// Ack acknowledges successful processing of the Message, signaling to the broker it's safe to delete.
//
// Ack is implemented by just forwarding the call to the underlying Message.AckFunc, panicking if it's not defined.
func (m *Message) Ack() {
	m.AckFunc()
}

// Nack acknowledges unsuccessful processing of the Message, signaling to the broker it's not safe to delete
// and that it should be resent at a later time.
//
// Nack is implemented by just forwarding the call to the underlying Message.NackFunc, panicking if it's not defined.
func (m *Message) Nack() {
	m.NackFunc()
}

// RecordIterator models an iterative pulling mechanism.
type RecordIterator interface {
	// NextRecord returns the next Record or an error.
	NextRecord(context.Context) (Record, error)
}

// Record defines the object used for communication with log-based brokers.
// Unlike Message, records are kept as an ordered log of events, and therefore do not have
// the concept of a negative acknowledgement; acknowledging a record implicitly acknowledges all records
// prior to it.
type Record struct {
	// Offset is the index of the record stored in a specific Partition.
	Offset int64

	// Partition is the index of the partition this record belongs to.
	// Partitions are a common way brokers store topics.
	Partition int64

	// Key identifies the payload part of the record (and not the record itself).
	// It doesn't have to be unique across the source topic and is mostly used
	// for ordering guarantees.
	Key string

	// Value holds the payload of the record.
	Value []byte

	// Attributes holds the properties commonly used by brokers to pass metadata.
	Attributes map[string]interface{}

	// PublishTime the time this message was published.
	PublishTime time.Time

	// IngestionTime the time this record was received.
	IngestionTime time.Time

	// CommitFunc must be called after successful processing (and not before).
	// This signals to the broker that this Record is safe to delete.
	CommitFunc func()
}

// Ack acknowledges successful processing of the Record, signaling to the broker it's safe to delete.
// It should be noted that in log-based brokers, acknowledging (i.e. committing an offset) has the side
// effect of implicitly acknowledging all records of the same Record.Partition with a lower value of Record.Offset.
//
// Ack is implemented by just forwarding the call to the underlying Record.CommitFunc, panicking if it's not defined.
func (r *Record) Ack() {
	r.CommitFunc()
}

// BatchedIterator models an iterative pulling mechanism.
// It is similar to RecordIterator, but returns a Batch instead, as the records contained
// in the batch can only be acknowledged as part of the batch.
type BatchedIterator interface {
	// NextBatch returns the next Batch or an error.
	NextBatch(context.Context) ([]Record, error)
}
