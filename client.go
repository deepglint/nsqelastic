package main

import (
	"encoding/json"
	"flag"
	"github.com/deepglint/nsqelastic/models"
	"github.com/franela/goreq"
	"github.com/nsqio/go-nsq"
	"log"
	"strconv"
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
	var v models.NodeItem
	body, _ := res.Body.ToString()
	log.Println(body)
	err = json.Unmarshal([]byte(body), &v)

	if err != nil {
		log.Fatal(err.Error())
	}
	addr = v.Ip + ":" + strconv.Itoa(v.Tcpport)
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
