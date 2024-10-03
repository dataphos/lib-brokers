lib-brokers - a universal consumer/producer library
===

lib-brokers is a Go library which contains the interfaces used to interact with messaging systems without relying on a
specific technology or client library.
This library attempts to solve the issue of properly abstracting away the interaction between applications and
messaging systems.

This is because these interfaces are unexpectedly difficult to get right, leading to issues like:

- Complicated interfaces which force implementations to duplicate code which has nothing to do with the actual operation
  the interfaces are supposed to abstract
- Business logic creeping into interface definition (things like message publishers sharing the message type with
  other, unrelated business logic)
- Concrete implementation creeping into interface definition, leading to interfaces which are too limiting and can’t be
  used in all cases, as the author of the interface had one specific implementation in mind, so the interface still
  contains implementation details of that initial implementation

Furthermore, there is the issue of the added cost since the time spent on implementing these interfaces is multiplied by
the number of teams which require an implementation. Therefore, not having a unified approach to interacting with
different messaging systems leads to the following issues as well:

- Time inefficiency
- Lack of testing
- Lack of proper documentation
- Poor knowledge sharing
- Possibility that the author of the implementation was not aware of some caveat of the technology in question
- Learning curve when switching/joining a product team
- Greater possibility of improper use of the interface (as a result of all the things mentioned above)

This is the motivation for designing a universal consumer/producer API: a Go library which would allow all existing and all
future products a unified view of a messaging system, pulling together resources and greatly improving knowledge
retention.

## Installation

`go get github.com/dataphos/lib-brokers`

Note that due to the external dependencies, Go version 1.18 or higher is required.


## Getting Started

Applications can either produce or consume messages from a messaging system, so this library is split into
two parts: the producer (publisher) API and the consumer API.

### Publishing

Producing (publishing) messages to a messaging system is rather straightforward, with the entire
API shown in the following snippet:

  ```go
type Publisher interface {
        // Topic constructs a new Topic.
        Topic(string) (Topic, error)
}

type Topic interface {
        // Publish publishes a Message.
        //
        // Blocks until the Message is received by the broker, the context is canceled or an error occurs.
        Publish(context.Context, OutboundMessage) error
        // BatchPublish publishes a batch.
        // 
        // Blocks until all the messages in the batch are received by the broker, the context is canceled or an error occurs.
        BatchPublish(context.Context, ...OutboundMessage) error
}

type OutboundMessage struct {
        // Key identifies the Data part of the message (and not the message itself).
        // It doesn't have to be unique across the topic and is mostly used for ordering guarantees.
        Key string
        
        // Data the payload of the message.
        Data []byte
        
        // Attributes holds the properties commonly used by brokers to pass metadata.
        Attributes map[string]interface{}
}
  ```

The `Publisher` is basically a factory of `Topic` instances. This is because most client libraries implement some sort
of internal batching mechanism. For example, Pub/Sub has a configurable batch size for publishing messages, so that not
every publish triggers an individual network request. This batch is kept at a topic level, so constructing new topic
instances for each message has substantial overhead.

For Kafka and Service Bus, BatchPublish may be more efficient than calling Publish for multiple messages in
separate goroutines.

An example of using this API can be found in the [examples](/examples) folder.

### Consuming

Consuming from messaging system, on the other hand, is a lot more complex; some messaging systems
have the concept of a negative acknowledge (Pub/Sub), while others don't (Kafka). The presence
of this concept also implies that that messaging system treats messages as individual entities, 
meaning acknowledgment operations have no side effect on any other message. Log-based messaging systems
like Kafka don't have this concept, so committing offsets in Kafka has side effects on all messages
in that partition prior to the "committed" one (they get implicitly committed).

Because of this, the consumer part of the API is split into two disjunctive groups:
the first concerns Pub/Sub-like messaging systems which have both positive and negative acknowledgment
operations (which means they trivially support concurrent processing as order is not important) and
Kafka-like messaging systems, which don't.  
Because of this, these two groups don't share the API and use different interfaces and a different
message type.

#### Individual Message Processing

At the core of the first group (the one where messages are individual entities) is the `Message` struct:

```go
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
```

There are six different consumer interfaces which, in some way, return the `Message` struct.  
However, the number of different interfaces is only there to keep the complexity of the broker-specific
implementations at a minimum; each individual messaging system implementation only needs to implement a single, "native"
interface (the one which is most alike the underlying client the implementation uses). 
The other interface implementations can be achieved through carefully written adapters found in the `brokerutil` package.

For example, the "main" interface for consuming individual messages is the `Receiver`, whose definition is shown below:

```go
type Receiver interface {
	// Receive runs the pulling process, blocking for the entire duration of the pull.
	//
	// Receive executes the provided callback for each pulled Message, meaning it is the responsibility
	// of the Receiver to provide the callbacks with multithreading.
	//
	// Receive blocks until either an error is returned, or when the provided ctx is canceled, in which case, nil is returned.
	Receive(context.Context, func(context.Context, Message)) error
}
```

You're used to consuming from Pub/Sub, so you use the `Receiver` interface, but now you want to also support ServiceBus,
which only implements `BatchedMessageIterator`.
You can achieve this in just a few lines of code, by first initializing the "native" ServiceBus consumer
object, and then pass it on to the appropriate `brokerutil` adapter, like so:

```go
iterator, err := servicebus.NewBatchIteratorFromEnv()
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
```

### Ordered Log Processing

At the core of the Kafka-like group are the `Record` and `Batch` message types:

```go
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

type Batch struct {
        // Records the records of the Batch. BatchRecord is identical to Record, but without the ability
	    // to commit itself (without CommitFunc).
        Records []BatchRecord
        
        // CommitFunc must be called after successful processing of the entire Batch, signaling to the
        // broker that this batch is safe to delete.
        CommitFunc func()
}
```

There are only two different consumer interfaces which use this structures. These interfaces
abstract what is effectively an iterator; `RecordIterator` returns a single record at a time, while `BatchedIterator` returns multiple.
Once again, there are two interfaces to simplify things for concrete implementations, and also because some
messaging systems can be used more efficiently if they return batches at a time.







