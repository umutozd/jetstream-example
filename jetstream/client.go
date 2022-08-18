package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/sirupsen/logrus"
)

const (
	// the number of replicas to be used for each stream
	// we need replicas so that publishing to streams don't fail
	streamReplicaCount = 3
)

type Message struct {
	Key         string
	Value       []byte
	Redelivered bool
	Timestamp   int64
	original    interface{}
}

type SubscribeFunc func(msg *Message) error

type PubSubClient interface {
	Subscribe(topic string, handler SubscribeFunc)
	Produce(ctx context.Context, topic string, value []byte) error
}

type jetStreamClient struct {
	uri string

	conn *nats.Conn
	js   nats.JetStreamContext
	nuid *nuid.NUID

	consumerGroup string
	consumerName  string

	streams map[string]*nats.StreamInfo

	lastPublishAt *time.Time
}

// NewJetStreamClient creates a new jetstream client with the given parameters and connects
// to the server(s) specified by uri.
func NewJetStreamClient(uri, consumerGroup, consumerName string) (PubSubClient, error) {
	jsc := &jetStreamClient{
		uri:           uri,
		consumerGroup: consumerGroup,
		consumerName:  consumerName,
		nuid:          nuid.New(),
	}

	var err error
	jsc.conn, err = nats.Connect(
		jsc.uri,
		nats.RetryOnFailedConnect(true),
		nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
			logrus.WithError(err).Warn("JetStream: client disconnected")
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logrus.Warn("JetStream: client reconnected")
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			logrus.Warn("JetStream: client connection closed")
		}),
	)

	if err != nil {
		return nil, fmt.Errorf("error connecting to nats: %v", err)
	}
	jsc.js, err = jsc.conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("error retrieving JetStreamContext from client: %v", err)
	}

	return jsc, nil
}

func (jsc *jetStreamClient) Subscribe(topic string, handler SubscribeFunc) {
	streamName := jsc.formatStreamName(topic)
	if err := jsc.createStreamIfNotExists(streamName, topic); err != nil {
		panic(fmt.Errorf("error initializing stream: %v", err))
	}

	// Create an explicit-ack consumer on the stream. This operation doesn't create a new one
	// if consumer already exists, but rather uses the existing one.
	_, err := jsc.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       jsc.consumerGroup,      // Durable is the sustained name where received messages are stored under.
		AckPolicy:     nats.AckExplicitPolicy, // Explicit ACKs are used to ACK or NAK all messages individually.
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	if err != nil {
		panic(fmt.Errorf("error adding consumer to stream: %v", err))
	}

	// Create a subscription on the consumer
	// other instances of this service create a subscription too and messages are distributed among all these instances
	sub, err := jsc.js.PullSubscribe(
		topic,                                    // all subjects in the stream
		jsc.consumerGroup,                        // durable name
		nats.Bind(streamName, jsc.consumerGroup), // bind a consumer to a stream explicitly based on a name
	)
	if err != nil {
		panic(fmt.Errorf("error creating pull-subscription on stream: %v", err))
	}

	go func() {
		for {
			messages, err := sub.Fetch(50)
			if err != nil {
				if errors.Is(err, nats.ErrTimeout) {
					continue
				}
				logrus.Errorf("error fetching from subscription: %v", err)
				continue
			}

			for _, msg := range messages {
				logger := logrus.WithFields(logrus.Fields{
					"subject": msg.Subject,
					"reply":   msg.Reply,
					"header":  msg.Header,
				})
				meta, err := msg.Metadata()
				if err != nil {
					logger.WithError(err).Error("PubSub: got malformed message")
					continue
				}
				logger = logger.WithFields(logrus.Fields{
					"sequence":      meta.Sequence.Stream,
					"timestamp":     meta.Timestamp,
					"num_delivered": meta.NumDelivered,
				})

				err = handler(&Message{
					Key:         fmt.Sprintf("%s-%d", msg.Subject, meta.Sequence.Stream),
					Value:       msg.Data,
					Timestamp:   meta.Timestamp.Unix(),
					Redelivered: meta.NumDelivered > 1,
					original:    msg,
				})
				if err != nil {
					if err := msg.Nak(); err != nil {
						logger.WithError(err).Errorf("unable to Nak jetstream message: %v", msg)
					}
				} else {
					// AckSync will ACK the message and request acknowledgement that the ACK is received by the server
					if err := msg.AckSync(); err != nil {
						logger.WithError(err).Errorf("unable to AckSync jetstream message %v", msg)
					}
				}
			}
		}
	}()
}

func (jsc *jetStreamClient) Produce(ctx context.Context, topic string, value []byte) error {
	streamName := jsc.formatStreamName(topic)
	if err := jsc.createStreamIfNotExists(streamName, topic); err != nil {
		return fmt.Errorf("unable to initialize stream: %v", err)
	}

	msgId := jsc.nuid.Next()
	opts := []nats.PubOpt{nats.MsgId(msgId), nats.RetryWait(time.Second * 5), nats.RetryAttempts(5)}
	if _, err := jsc.js.Publish(topic, value, opts...); err != nil {
		return fmt.Errorf("error producing message: %v", err)
	}
	return nil
}

// formatStreamName returns the NATS-compliant stream name from the given one.
// Specifically, NATS doesn't allow "." characters in stream names, but our stream names
// do contain them (i.e. protobuf message names). formatStreamName replaces them with "-".
func (jsc *jetStreamClient) formatStreamName(streamName string) string {
	return strings.ReplaceAll(streamName, ".", "-")
}

// createStreamIfNotExists creates a stream if it doesn't exist. If it exists, it checks its
// configuration.
func (jsc *jetStreamClient) createStreamIfNotExists(streamName, topic string) error {
	if _, ok := jsc.streams[streamName]; ok {
		return nil
	}
	stream, err := jsc.js.StreamInfo(streamName)
	if err == nil {
		// stream exists, its name must equal its one and only topic
		if len(stream.Config.Subjects) != 1 {
			return fmt.Errorf("stream contains multiple topics, but it must contain only one")
		}
		if stream.Config.Subjects[0] != topic {
			return fmt.Errorf("stream contains an invalid topic: got=%s, expected=%s", stream.Config.Subjects[0], topic)
		}
		if stream.Config.Replicas != streamReplicaCount {
			return fmt.Errorf("stream config has %d replicas, but %d is needed", stream.Config.Replicas, streamReplicaCount)
		}
		jsc.streams[streamName] = stream
		return nil
	}
	if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("unable to get stream info: %v", err)
	}

	// stream doesn't exists, create one
	stream, err = jsc.js.AddStream(&nats.StreamConfig{
		Name:       streamName,           // stream name is a unique identifier for the stream
		Storage:    nats.FileStorage,     // stream messages are stored in file systemx
		Subjects:   []string{topic},      // the topic of the stream (in our system, we use one subject per stream even though multiple is possible)
		Duplicates: time.Minute * 5,      // duplicate window in which duplicate messages are tracked (messages with same ids are deduplicated when they are published in this window)
		Retention:  nats.WorkQueuePolicy, // messages are kept in storage until they are delivered and explicitly acknowledged
		Replicas:   streamReplicaCount,
	})
	if err != nil {
		return fmt.Errorf("error creating stream: %v", err)
	}
	jsc.streams[streamName] = stream
	return nil
}
