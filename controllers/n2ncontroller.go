package controllers

import (
	"bufio"
	//"bytes"
	"encoding/json"
	"errors"
	"github.com/nsqio/go-nsq"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type N2nController struct {
	route     map[string]bool
	exitchan  chan bool
	msgchan   chan *nsq.Message
	producers map[string]*nsq.Producer
	consumers map[string]*nsq.Consumer
	mutex     *sync.Mutex
	mutex2    *sync.Mutex
	mutex3    *sync.Mutex
	ccfg      *nsq.Config
	pcfg      *nsq.Config
}

func NewN2nController() *N2nController {
	v := new(N2nController)
	v.route = make(map[string]bool)

	v.exitchan = make(chan bool)

	v.msgchan = make(chan *nsq.Message, 10000)

	v.producers = make(map[string]*nsq.Producer)

	v.consumers = make(map[string]*nsq.Consumer)

	v.mutex = &sync.Mutex{}

	v.mutex2 = &sync.Mutex{}

	v.mutex3 = &sync.Mutex{}

	v.ccfg = nsq.NewConfig()

	v.pcfg = nsq.NewConfig()
	return v

}
func (this *N2nController) HandleMessage(m *nsq.Message) error {
	log.Println("New Msg")
	this.msgchan <- m
	return nil
}

func (this *N2nController) AddSource(addr string, topic string, channel string) error {
	this.mutex.Lock()
	log.Println(1)
	if this.consumers[addr] != nil {
		log.Println("The Consumer has existed")
		this.mutex.Unlock()
		return errors.New("The Consumer has existed")
	}
	log.Println(2)
	c, err := nsq.NewConsumer(topic, channel, this.ccfg)
	if err != nil {
		log.Println(err)
		this.mutex.Unlock()
		return err
	}
	log.Println(3)
	c.AddHandler(nsq.HandlerFunc(this.HandleMessage))
	err = c.ConnectToNSQD(addr)
	if err != nil {
		this.mutex.Unlock()
		return err
	}
	this.consumers[addr] = c
	this.mutex.Unlock()
	return nil
}

func (this *N2nController) AddNode(addr string) {
	this.mutex2.Lock()
	if this.producers[addr] != nil {
		log.Println("The Producer has existed")
		this.mutex2.Unlock()
		return
	}
	p, err := nsq.NewProducer(addr, this.pcfg)
	if err != nil {
		log.Println(err)
		this.mutex2.Unlock()
		return
	}
	this.producers[addr] = p
	this.mutex2.Unlock()
}

func (this *N2nController) DelNode(addr string) {
	//var pr *nsq.Producer
	this.mutex2.Lock()
	if this.producers[addr] != nil {
		//pr = producers[addr]
		this.producers[addr] = nil
	}
	this.mutex2.Unlock()
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
func (this *N2nController) RouteMsg() {
	for {
		select {
		case m := <-this.msgchan:
			t := GetTopic(m)
			for k := range this.producers {
				this.mutex3.Lock()
				if this.route[k+t] == false {
					this.mutex3.Unlock()
					continue
				}
				this.mutex3.Unlock()
				this.producers[k].Publish(t, m.Body)
			}
		}
	}

}

func (this *N2nController) DelTopic(node string, topic string) {
	this.mutex3.Lock()
	this.route[node+topic] = false
	this.mutex3.Unlock()
}

func (this *N2nController) AddTopic(node string, topic string) {
	this.mutex3.Lock()
	this.route[node+topic] = true
	this.mutex3.Unlock()
}
func (this *N2nController) handleAddSource(w http.ResponseWriter, r *http.Request) {
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
	err = this.AddSource(v.Addr, v.Topic, v.Channel)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	w.Write([]byte("Success"))
}

func (this *N2nController) handleAddNode(w http.ResponseWriter, r *http.Request) {
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
	this.AddNode(v.Addr)
	w.Write([]byte("Success"))
}
func (this *N2nController) handleDelNode(w http.ResponseWriter, r *http.Request) {
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
	this.DelNode(v.Addr)
	w.Write([]byte("Success"))
}

func (this *N2nController) handleDelTopic(w http.ResponseWriter, r *http.Request) {
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
	this.DelTopic(v.Addr, v.Topic)
	w.Write([]byte("Success"))
}
func (this *N2nController) handleAddTopic(w http.ResponseWriter, r *http.Request) {
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
	this.AddTopic(v.Addr, v.Topic)
	w.Write([]byte("Success"))
}
func (this *N2nController) ping(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello world"))
}
func (this *N2nController) Start() {
	go this.RouteMsg()
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/addsource", this.handleAddSource)
		mux.HandleFunc("/api/addnode", this.handleAddNode)
		mux.HandleFunc("/api/delnode", this.handleDelNode)
		mux.HandleFunc("/api/deltopic", this.handleDelTopic)
		mux.HandleFunc("/api/addtopic", this.handleAddTopic)
		mux.HandleFunc("/api/ping", this.ping)
		http.ListenAndServe("0.0.0.0:3003", mux)
	}()
}
