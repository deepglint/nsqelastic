package main

import (
	"bufio"
	//"bytes"
	"encoding/json"
	"errors"
	"flag"
	"github.com/nsqio/go-nsq"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

var route = make(map[string]bool)

var exitchan = make(chan bool)

var msgchan = make(chan *nsq.Message, 10000)

var producers = make(map[string]*nsq.Producer)

var consumers = make(map[string]*nsq.Consumer)

var mutex = &sync.Mutex{}

var mutex2 = &sync.Mutex{}

var mutex3 = &sync.Mutex{}

var ccfg = nsq.NewConfig()

var pcfg = nsq.NewConfig()

//var bufscan = bufio.NewScanner(r)
var mainnode = flag.String("mainnode", "127.0.0.1:4152", "The Main Node")

func HandleMessage(m *nsq.Message) error {
	log.Println("New Msg")
	msgchan <- m
	return nil
}
func AddSource(addr string, topic string, channel string) error {
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

func AddNode(addr string) {
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

func DelNode(addr string) {
	//var pr *nsq.Producer
	mutex2.Lock()
	if producers[addr] != nil {
		//pr = producers[addr]
		producers[addr] = nil
	}
	mutex2.Unlock()
	//pr = nil
}

func GetTopic(m *nsq.Message) string {
	body := m.Body
	a, b, _ := bufio.ScanLines(body, false)
	//log.Println(a, string(b), c)
	m.Body = m.Body[a:]
	//bytes.TrimPrefix(m.Body, b)
	return string(b)
}

func RouteMsg() {
	for {
		m := <-msgchan
		t := GetTopic(m)
		for k := range producers {
			if k == *mainnode {
				producers[k].Publish(t, m.Body)
				continue
			}
			mutex3.Lock()
			if route[k+t] == false {
				mutex3.Unlock()
				continue
			}
			mutex3.Unlock()
			producers[k].Publish(t, m.Body)
		}
	}
}

func DelTopic(node string, topic string) {
	if node == *mainnode {
		return
	}
	mutex3.Lock()
	route[node+topic] = false
	mutex3.Unlock()
}

func AddTopic(node string, topic string) {
	if node == *mainnode {
		return
	}
	mutex3.Lock()
	route[node+topic] = true
	mutex3.Unlock()
}

func handleAddSource(w http.ResponseWriter, r *http.Request) {
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
	err = AddSource(v.Addr, v.Topic, v.Channel)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	w.Write([]byte("Success"))
}

func handleAddNode(w http.ResponseWriter, r *http.Request) {
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
	AddNode(v.Addr)
	w.Write([]byte("Success"))
}

func handleDelNode(w http.ResponseWriter, r *http.Request) {
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
	DelNode(v.Addr)
	w.Write([]byte("Success"))
}

func handleDelTopic(w http.ResponseWriter, r *http.Request) {
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
	DelTopic(v.Addr, v.Topic)
	w.Write([]byte("Success"))
}

func handleAddTopic(w http.ResponseWriter, r *http.Request) {
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
	AddTopic(v.Addr, v.Topic)
	w.Write([]byte("Success"))
}

func ping(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello world"))
}
func main() {
	flag.Parse()
	go RouteMsg()

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/addsource", handleAddSource)
		mux.HandleFunc("/api/addnode", handleAddNode)
		mux.HandleFunc("/api/delnode", handleDelNode)
		mux.HandleFunc("/api/deltopic", handleDelTopic)
		mux.HandleFunc("/api/addtopic", handleAddTopic)
		mux.HandleFunc("/api/ping", ping)
		http.ListenAndServe("0.0.0.0:3003", mux)
	}()
	<-exitchan
}
