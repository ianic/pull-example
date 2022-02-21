package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	subject = "foo"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile | log.Lmicroseconds)

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}

	ts := time.Now().Unix()
	for i := 0; i < 1024; i++ {
		data := fmt.Sprintf("msg no %d %d", ts, i)
		if err := nc.Publish(subject, []byte(data)); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("pub %s %s\n", subject, data)
		//time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
	}

	nc.Flush()
}
