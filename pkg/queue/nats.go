package queue

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func Connect() (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("ContribPulse Client"),
		nats.Timeout(10 * time.Second),
		nats.ReconnectWait(5 * time.Second),
		nats.MaxReconnects(5),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("NATS client disconnected due to error: %s", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS client reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("NATS client connection is closed")
		}),
	}

	nc, err := nats.Connect("nats://nats:4222", opts...)
	if err != nil {
		return nil, err
	}

	log.Printf("Connected to NATS at %s", nc.ConnectedUrl())
	return nc, nil
}
