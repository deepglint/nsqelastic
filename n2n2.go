package main

import (
	"encoding/json"
	"errors"
	"github.com/nsqio/go-nsq"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

var routeforbid = make(map[string]bool)

var exitchan = make(chan bool)

var msgchan = make(chan *nsq.Message, 10000)

var producers = make(map[string]*nsq.Producer)

var consumers = make(map[string]*nsq.Consumer)

var mutex = &sync.Mutex{}

var mutex2 = &sync.Mutex{}

var mutex3 = &sync.Mutex{}

var ccfg = nsq.NewConfig()

var pcfg = nsq.NewConfig()

func HandleMessage(m *nsq.Message) error {
	log.Println("New Msg")
	msgchan <- m
	return nil
}
func AddConsumer(addr string, topic string, channel string) error {
	mutex.Lock()
	log.Println(1)
	if consumers[addr] != nil {
		log.Println("The Consumer has existed")
		mutex.Unlock()
		return errors.New("The Consumer has existed")
	}
	log.Println(2)
	c, err := nsq.NewConsumer(topic, channel, ccfg)
	if err != nil {
		log.Println(err)
		mutex.Unlock()
		return err
	}
	log.Println(3)
	c.AddHandler(nsq.HandlerFunc(HandleMessage))
	err = c.ConnectToNSQD(addr)
	if err != nil {
		mutex.Unlock()
		return err
	}
	consumers[addr] = c
	mutex.Unlock()
	return nil
}

func AddProducer(addr string) {
	mutex2.Lock()
	if producers[addr] != nil {
		log.Println("The Producer has existed")
		mutex2.Unlock()
		return
	}
	p, err := nsq.NewProducer(addr, pcfg)
	if err != nil {
		log.Println(err)
		mutex2.Unlock()
		return
	}
	producers[addr] = p
	mutex2.Unlock()
}

func GetTopic(m *nsq.Message) string {
	return "testing"
}

func RouteMsg() {
	for {
		m := <-msgchan
		t := GetTopic(m)
		for k := range producers {
			mutex3.Lock()
			if routeforbid[k+t] == true {
				mutex3.Unlock()
				continue
			}
			mutex3.Unlock()
			producers[k].Publish(t, m.Body)
		}
	}
}

func Forbid(node string, topic string) {
	mutex3.Lock()
	routeforbid[node+topic] = true
	mutex3.Unlock()
}

func UnForbid(node string, topic string) {
	mutex3.Lock()
	routeforbid[node+topic] = false
	mutex3.Unlock()
}

func handleAddConsumer(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	var v = struct {
		Addr    string `json:addr`
		Topic   string `json:topic`
		Channel string `json:channel`
	}{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	log.Println("Adding Consumer", v)
	err = AddConsumer(v.Addr, v.Topic, v.Channel)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	w.Write([]byte("Success"))
}

func handleAddProducer(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	var v = struct {
		Addr string `json:addr`
	}{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	AddProducer(v.Addr)
	w.Write([]byte("Success"))
}

func handleForbidNode(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	var v = struct {
		Addr  string `json:addr`
		Topic string `josn:topic`
	}{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	Forbid(v.Addr, v.Topic)
	w.Write([]byte("Success"))
}

func handleUnForbidNode(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	var v = struct {
		Addr  string `json:addr`
		Topic string `josn:topic`
	}{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	UnForbid(v.Addr, v.Topic)
	w.Write([]byte("Success"))
}

func ping(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello world"))
}
func main() {

	go RouteMsg()

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/addconsumer", handleAddConsumer)
		mux.HandleFunc("/api/addproducer", handleAddProducer)
		mux.HandleFunc("/api/forbidnode", handleForbidNode)
		mux.HandleFunc("/api/unforbidnode", handleUnForbidNode)
		mux.HandleFunc("/api/ping", ping)
		http.ListenAndServe("0.0.0.0:3003", mux)
	}()
	<-exitchan
}
