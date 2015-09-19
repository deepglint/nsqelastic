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
	nsqElastic.WaitGroup.Wrap(func() { nsqElastic.LookupLoop() })
	nsqElastic.WaitGroup.Wait()
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
	mux.HandleFunc("/debug", this.HttpController.Debug)

	this.logf("Http server listens on %s\n", this.Config.HttpAddr)
	http.ListenAndServe(this.Config.HttpAddr, mux)
}

func (this *NsqElastic) LookupLoop() {
	ticker := time.Tick(10 * time.Second)

	for {
		select {
		case <-ticker:
			err := this.lookupUpdate()
			if err != nil {
				this.logf("timer error-%v\n", err)
			}
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
	//nodes.Data.Producers = make([]models.Node_lookup, 0)

	if err = json.Unmarshal(result, &nodes); err != nil {
		return err
	}
	if nodes.Status_code != 200 {
		return errors.New(nodes.Status_txt)
	}
	TopicChan2NodeItemMap := models.NewTable()
	Topic2TopicItemMap := models.NewTable()
	NodeItem2NodeMap := models.NewTable()
	//itemsList := make(models.NodeItem, 0)
	var WaitGroup util.WaitGroupWrapper
	for i := 0; i < len(nodes.Data.Producers); i++ {
		var item models.NodeItem
		item.Httpport = nodes.Data.Producers[i].HTTPPort
		item.Tcpport = nodes.Data.Producers[i].TCPPort
		item.Ip = nodes.Data.Producers[i].RemoteAddress
		if strings.Contains(item.Ip, ":") {
			item.Ip = strings.Split(item.Ip, ":")[0]
		}
		//itemsList = append(models.NodeItem, item)
		WaitGroup.Wrap(func() {
			n.nsqdUpdate(TopicChan2NodeItemMap, Topic2TopicItemMap, NodeItem2NodeMap, item)
		})
	}
	WaitGroup.Wait()
	n.NodeItem2NodeMap.Replace(NodeItem2NodeMap)
	n.Topic2TopicItemMap.Replace(Topic2TopicItemMap)
	n.TopicChan2NodeItemMap.Replace(TopicChan2NodeItemMap)
	//fmt.Printf("new %v \n", TopicChan2NodeItemMap)
	return nil
}
func (n *NsqElastic) nsqdUpdate(topicChan2NodeItemMap, topic2TopicItemMap, nodemap *models.BigTable, nodeitem models.NodeItem) error {
	fmt.Printf("nsqd timer\n")
	u, _ := url.Parse(fmt.Sprintf("http://%s:%d/stats?format=json", nodeitem.Ip, nodeitem.Httpport))

	//fmt.Printf("rs:%s\n", fmt.Sprintf("http://%s:%d/stats", nodeitem.Ip, nodeitem.Httpport))
	res, err := http.Get(u.String())
	if err != nil {
		return err
	}
	//fmt.Printf("rs:%s\n", "s3")
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}
	var nodes models.NodeInfo_nsqd
	//fmt.Printf("rs:%s\n", string(result))
	if err := json.Unmarshal(result, &nodes); err != nil {
		return err
	}
	//fmt.Println("ssssss")
	if nodes.Status_code != 200 {
		return errors.New(nodes.Status_txt)
	}

	var onenode models.Node
	onenode.NodeItem = nodeitem

	for i := 0; i < len(nodes.Data.Topics); i++ {
		var topicItem struct {
			TopicName string
			Channels  []struct {
				ChanName string
			}
		}
		topicItem.TopicName = nodes.Data.Topics[i].TopicName
		TopicItemMap := topic2TopicItemMap.Get(topicItem.TopicName)
		if TopicItemMap == nil {
			tlist := make([]models.TopicItem, 0)
			t := models.TopicItem{}
			t.TopicName = topicItem.TopicName
			t.NodeItem = nodeitem
			t.Chancount = len(nodes.Data.Topics[i].Channels)
			tlist = append(tlist, t)
			topic2TopicItemMap.Update(topicItem.TopicName, tlist)
		} else {
			tlist, _ := TopicItemMap.([]models.TopicItem)
			t := models.TopicItem{}
			t.TopicName = topicItem.TopicName
			t.NodeItem = nodeitem
			t.Chancount = len(nodes.Data.Topics[i].Channels)
			tlist = append(tlist, t)
		}
		for ii := 0; ii < len(nodes.Data.Topics[i].Channels); ii++ {
			var tc struct {
				ChanName string
			}
			tc.ChanName = nodes.Data.Topics[i].Channels[ii].ChannelName
			topicItem.Channels = append(topicItem.Channels, tc)
			var tempTC models.TopicChannel
			tempTC.Channel = tc.ChanName
			tempTC.Topic = topicItem.TopicName
			topicChan2NodeItemMap.Update(tempTC, nodeitem)
		}

		onenode.Topics = append(onenode.Topics, topicItem)
	}
	nodemap.Update(nodeitem, onenode)
	return nil
}
