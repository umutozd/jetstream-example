package main

import (
	"context"
	"encoding/json"
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

	type PublishMessage struct {
		Count int `json:"count,omitempty"`
	}

	for i := 0; i < 3000; i++ {
		sop := PublishMessage{
			Count: i,
		}
		b, err := json.Marshal(sop)
		if err != nil {
			logrus.WithError(err).Error("error marshaling SendOtsimoPush")
			continue
		}
		if err := cli.Produce(context.TODO(), *topic, b); err != nil {
			logrus.WithError(err).Errorf("stressPublisher: error publishing message %d", i)
		}

		if i%10 == 0 {
			logrus.Infof("publisher: sleeping at index=%d", i)
			time.Sleep(time.Millisecond * 25)
		}
	}
}
