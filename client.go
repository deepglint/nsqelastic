package main

import (
	"flag"
	"github.com/franela/goreq"
	"github.com/nsqio/go-nsq"
	"log"
)

var (
	eaddr    = flag.String("eaddr", "", "The elastic addr")
	topic    = flag.String("topic", "", "The topic you wanna sub")
	channel  = flag.String("channel", "", "The channel you wanna sub")
	consumer *nsq.Consumer
	cfg      = nsq.NewConfig()
	addr     string
	exitchan chan bool
)

func main() {
	flag.Parse()
	if *eaddr == "" || *topic == "" || *channel == "" {
		log.Println("Please input the elastic addr or topic or channel")
		return
	}
	res, err := goreq.Request{Uri: *eaddr + "?topic=" + *topic + "&channel=" + *channel}.Do()
	if err != nil {
		log.Println(err.Error())
	}
	body, _ := res.Body.ToString()
	log.Println(body)

	consumer, err = nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err.Error())
	}
	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		log.Println(string(m.Body))
		return nil
	}))
	consumer.ConnectToNSQD(addr)
	<-exitchan
}
