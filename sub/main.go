package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	consumerName := streamName
	subjects := []string{"foo", "bar"}

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}

	mgr, err := jsm.New(nc)
	if err != nil {
		return fmt.Errorf("jsm.New failed %w", err)
	}
	st, err := mgr.LoadOrNewStream(streamName,
		jsm.Subjects(subjects...),
	)

	cs, err := st.LoadOrNewConsumer(streamName,
		jsm.DurableName(consumerName),
		jsm.DeliverAllAvailable(),
		jsm.AcknowledgeExplicit(),
	)
	if err != nil {
		return fmt.Errorf("NewConsumer failed %w", err)
	}

	inboxSubject := nats.NewInbox()
	fmt.Printf("inbox: %s\n", inboxSubject)
	inbox := make(chan *nats.Msg, 1024)
	sub, err := nc.ChanSubscribe(inboxSubject, inbox)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	ctx := interuptContext()
	done := make(chan struct{})
	go func() {
		defer close(done)

		inboxCap := cap(inbox)
		requested := 0
		consumed := 0

		for {
			//if inInbox := (requested - consumed); inInbox < inboxCap/2 {
			if consumed == requested {
				cs.NextMsgRequest(inboxSubject, &api.JSApiConsumerGetNextRequest{
					Expires: time.Second * 10,
					Batch:   inboxCap,
					NoWait:  true,
				})
				requested += inboxCap
			}

			select {
			case <-ctx.Done():
				return
			case nm := <-inbox:
				if isControlMsg(nm) {
					return
				}
				fmt.Printf("msg %s\n", nm.Data)
				_ = nm.Ack()
				consumed++
			default:
			}

		}
	}()

	<-done
	sub.Unsubscribe()
	close(inbox)
	for nm := range inbox {
		_ = nm.Nak()
	}
	return nil
}

func isControlMsg(nm *nats.Msg) bool {
	is := len(nm.Data) == 0 && len(nm.Header.Get("Status")) == 3
	if is {
		fmt.Printf("control message: %s %s\n", nm.Header.Get("Status"), nm.Header.Get("Description"))
	}
	return is
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
