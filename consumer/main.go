package main

import (
	"flag"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/umutozd/jetstream-example/jetstream"
)

const (
	uri = "nats://192.168.0.72:4222,nats://192.168.0.72:4223,nats://192.168.0.9:4224"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	consumerGroup := flag.String("group", "", "consumer group")
	consumerName := flag.String("name", "", "consumer name")
	topic := flag.String("topic", "", "topic to subscribe to")
	serverURI := flag.String("uri", uri, "server uri, possible separated by comma to indicate multiple servers")

	flag.Parse()
	logrus.Infof("group=%s, name=%s, topic=%s, uri=%q", *consumerGroup, *consumerName, *topic, *serverURI)

	cli, err := jetstream.NewJetStreamClient(*serverURI, *consumerGroup, *consumerName)
	if err != nil {
		logrus.WithError(err).Fatal("error creating jetstream client")
	}
	cli.Subscribe(*topic, func(msg *jetstream.Message) error {
		logrus.WithFields(logrus.Fields{
			"key":         msg.Key,
			"value":       string(msg.Value),
			"timestamp":   msg.Timestamp,
			"redelivered": msg.Redelivered,
		}).Info("received message")
		return nil
	})

	time.Sleep(time.Minute * 20) // sleep so that background subscription handler function can run
}
