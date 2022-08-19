module github.com/umutozd/jetstream-example

go 1.18

require (
	github.com/nats-io/nats.go v1.16.0
	github.com/nats-io/nuid v1.0.1
	github.com/sirupsen/logrus v1.9.0
)

require (
	github.com/konsorten/go-windows-terminal-sequences v1.0.1 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	golang.org/x/crypto v0.0.0-20210314154223-e6e6c4f2bb5b // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
)

replace github.com/nats-io/nats.go => github.com/umutozd/nats.go v0.0.0
