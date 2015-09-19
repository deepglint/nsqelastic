package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/deepglint/nsqelastic/controllers"
	"github.com/deepglint/nsqelastic/models"
	"github.com/deepglint/nsqelastic/util"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type NsqElastic struct {
	Config                *models.ConfigModel
	HttpController        *controllers.NsqController
	WaitGroup             util.WaitGroupWrapper
	TopicChan2NodeItemMap *models.BigTable
	Topic2TopicItemMap    *models.BigTable
	NodeItem2NodeMap      *models.BigTable
}

func main() {
	config := &models.ConfigModel{
		NsqdAddr:        "127.0.0.1:4151",
		HttpAddr:        ":4131",
		LookupdAddr:     "127.0.0.1:4161",
		MasterTopic:     "master_topic",
		TopicMaxChannel: 20,
	}
	nsqElastic := NewNsqElastic(config)
	nsqElastic.WaitGroup.Wrap(func() { nsqElastic.HttpServe() })
}
func NewNsqElastic(config *models.ConfigModel) *NsqElastic {
	topicChan2NodeItemMap := models.NewTable()
	topic2TopicItemMap := models.NewTable()
	nodeItem2NodeMap := models.NewTable()
	c := controllers.NewNsqController(config, topicChan2NodeItemMap, topic2TopicItemMap, nodeItem2NodeMap)
	n := &NsqElastic{
		Config:                config,
		HttpController:        c,
		TopicChan2NodeItemMap: topicChan2NodeItemMap,
		Topic2TopicItemMap:    topic2TopicItemMap,
		NodeItem2NodeMap:      nodeItem2NodeMap,
	}
	n.logf("New NsqElastic")
	return n
}

func (this *NsqElastic) logf(f string, args ...interface{}) {
	// if l.opts.Logger == nil {
	// 	return
	// }
	// l.opts.Logger.Output(2, fmt.Sprintf(f, args...))
	fmt.Printf(f, args...)
}

func (this *NsqElastic) HttpServe() {
	mux := http.NewServeMux()
	mux.HandleFunc("/pub", this.HttpController.Pub)
	mux.HandleFunc("/sub", this.HttpController.Sub)

	this.logf("Http server listens on %s\n", this.Config.HttpAddr)
	http.ListenAndServe(this.Config.HttpAddr, mux)
}

func (this *NsqElastic) LookupLoop() {
	ticker := time.Tick(1 * time.Second)
	for {
		select {
		case <-ticker:
			this.lookupUpdate()
		}
	}
}

func (n *NsqElastic) lookupUpdate() error {
	u, _ := url.Parse(fmt.Sprintf("http://%s/nodes", n.Config.LookupdAddr))
	res, err := http.Get(u.String())
	if err != nil {
		return err
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}
	var nodes models.NodeInfo_lookup
	if err := json.Unmarshal(result, nodes); err != nil {
		return err
	}
	if nodes.Status_code != 200 {
		return errors.New(nodes.Status_txt)
	}
	// TopicChan2NodeItemMap := make(map[interface{}]interface{})
	// Topic2TopicItemMap := make(map[interface{}]interface{})
	// NodeItem2NodeMap := make(map[interface{}]interface{})

	for i := 0; i < len(nodes.Data.Producers); i++ {
		var item models.NodeItem
		item.Httpport = nodes.Data.Producers[i].HTTPPort
		item.Tcpport = nodes.Data.Producers[i].TCPPort
		item.Ip = nodes.Data.Producers[i].RemoteAddress
		if strings.Contains(item.Ip, ":") {
			item.Ip = strings.Split(item.Ip, ":")[0]
		}

	}
	return nil
}
