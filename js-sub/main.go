package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	streamName := "foo"
	consumerName := "foojs"
	subjects := []string{"foo", "bar"}

	// connect to NATS server
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}

	// create
	mgr, err := jsm.New(nc)
	if err != nil {
		return fmt.Errorf("jsm.New failed %w", err)
	}
	st, err := mgr.LoadOrNewStream(streamName,
		jsm.Subjects(subjects...),
	)
	if err != nil {
		return fmt.Errorf("LoadOrNewStream failed %w", err)
	}

	inbox := make(chan *nats.Msg, 16)
	maxAckPending := cap(inbox)
	var inboxSubject string

	// load or create new consumer
	if cs, err := st.LoadConsumer(consumerName); err == nil {
		inboxSubject = cs.DeliverySubject()
		// check max pending consumer configuration
		if cs.MaxAckPending() != maxAckPending {
			if err := cs.UpdateConfiguration(jsm.MaxAckPending(uint(maxAckPending))); err != nil {
				return fmt.Errorf("consumer UpdateConfiguration failed %w", err)
			}
		}
	} else {
		ae := api.ApiError{}
		if errors.As(err, &ae) && ae.NotFoundError() {
			fmt.Printf("ae: %#v", ae)
			// consumer don't exists create it
			inboxSubject = nats.NewInbox()
			_, err := st.NewConsumer(
				jsm.DurableName(consumerName),
				jsm.DeliverySubject(inboxSubject),
				jsm.DeliverAllAvailable(),
				jsm.AcknowledgeExplicit(),
				jsm.MaxAckPending(uint(maxAckPending)),
			)
			if err != nil {
				return fmt.Errorf("NewConsumer failed %w", err)
			}
		} else {
			return fmt.Errorf("LoadConsumer failed %w", err)
		}
	}
	fmt.Printf("inbox subject: %s\n", inboxSubject)

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	sub, err := js.ChanSubscribe("bar", inbox, nats.Bind(streamName, consumerName))
	if err != nil {
		return err
	}
	//sub.SetPendingLimits(maxAckPending, 5*1024*1024)

	go func() {
		<-interuptContext().Done()
		sub.Unsubscribe()
		close(inbox)
	}()

	i := 0
	for nm := range inbox {
		mm, err := nm.Metadata()
		if err != nil {
			return err
		}
		fmt.Printf("%d pending: %d, sequence: %d %d\n", i, mm.NumPending, mm.Sequence.Consumer, mm.Sequence.Stream)
		_ = nm.Ack()
		i++
	}

	return nil
}

// InteruptContext returns context which will be closed on application interupt
func interuptContext() context.Context {
	ctx, stop := context.WithCancel(context.Background())
	go func() {
		c := make(chan os.Signal, 1)
		//SIGINT je ctrl-C u shell-u, SIGTERM salje upstart kada se napravi sudo stop ...
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		stop()
	}()
	return ctx
}
