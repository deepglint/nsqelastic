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
	"strconv"
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
	NodeSourceList        []models.NodeState
}

func main() {
	config := &models.ConfigModel{
		NsqdAddr:        "127.0.0.1:4151",
		HttpAddr:        ":4131",
		LookupdAddr:     "127.0.0.1:4161",
		MasterTopic:     "master_topic",
		TopicMaxChannel: 20,
		N2n2Addr:        "127.0.0.1:3003", //[] int {1,2,3 }
		NodeList:        []string{"127.0.0.1:4150/4151"},
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
	nodeSourceList := make([]models.NodeState, 0)
	c := controllers.NewNsqController(config, topicChan2NodeItemMap, topic2TopicItemMap, nodeItem2NodeMap, nodeSourceList)
	n := &NsqElastic{
		Config:                config,
		HttpController:        c,
		TopicChan2NodeItemMap: topicChan2NodeItemMap,
		Topic2TopicItemMap:    topic2TopicItemMap,
		NodeItem2NodeMap:      nodeItem2NodeMap,
		NodeSourceList:        nodeSourceList,
	}

	for i := 0; i < len(config.NodeList); i++ {
		err :=
			n.addSourceByString(config.NodeList[i])
		if err != nil {
			n.logf(err.Error() + "\n")
		}
	}
	n.logf("New NsqElastic")
	return n
}
func (n *NsqElastic) addSourceByString(str string) error {
	if n.NodeSourceList == nil {
		n.NodeSourceList =
			make([]models.NodeState, 0)
	}
	ipPorts := strings.Split(str, ":")
	if len(ipPorts) < 2 {
		return errors.New("error node source")
	}
	ip := ipPorts[0]
	ports := strings.Split(ipPorts[1], "/")
	if len(ports) < 2 {
		return errors.New("error node source")
	}
	tcp := ports[0]
	tcpInt, err := strconv.Atoi(tcp)
	if err != nil {
		return err
	}
	http := ports[1]
	httpInt, err_ := strconv.Atoi(http)
	if err_ != nil {
		return err_
	}
	nodeState := models.NodeState{
		NodeItem: models.NodeItem{Ip: ip,
			Httpport: httpInt,
			Tcpport:  tcpInt},
		Stats: models.UNUSE,
	}
	n.NodeSourceList = append(n.NodeSourceList, nodeState)
	return nil
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
	//todo : update sourceList
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
		find := false
		del := false
		for loopi := 0; loopi < len(n.NodeSourceList); loopi++ {
			if n.NodeSourceList[loopi].NodeItem.Eq(item) {
				find = true

				if n.NodeSourceList[loopi].Stats == models.DEL { //delete
					del = true
					break
				} else {
					n.NodeSourceList[loopi].Stats = models.USING
				}
			}

		}
		if !find {
			nodeState := models.NodeState{
				NodeItem: item,
				Stats:    models.USING,
			}
			n.NodeSourceList = append(n.NodeSourceList, nodeState)
		}
		if del {
			continue
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

	if len(nodes.Data.Topics) <= 0 { //delete node
		for i := 0; i < len(n.NodeSourceList); i++ {
			if n.NodeSourceList[i].NodeItem.Eq(nodeitem) { //delete
				n.NodeSourceList[i].Stats = models.DEL
				break
			}
		}
		return nil
	}
	for i := 0; i < len(nodes.Data.Topics); i++ {
		if len(nodes.Data.Topics[i].Channels) <= 0 { //delete topic
			//todo:delete topic
			continue
		}
		var nodeTopicItem struct {
			TopicName string
			Channels  []struct {
				ChanName string
			}
		}
		nodeTopicItem.TopicName = nodes.Data.Topics[i].TopicName
		TopicItemMap := topic2TopicItemMap.Get(nodeTopicItem.TopicName)
		if TopicItemMap == nil {
			tlist := make([]models.TopicItem, 0)
			t := models.TopicItem{}
			t.TopicName = nodeTopicItem.TopicName
			t.NodeItem = nodeitem
			t.Chancount = len(nodes.Data.Topics[i].Channels)
			tlist = append(tlist, t)
			topic2TopicItemMap.Update(nodeTopicItem.TopicName, tlist)
		} else {
			tlist, _ := TopicItemMap.([]models.TopicItem)
			t := models.TopicItem{}
			t.TopicName = nodeTopicItem.TopicName
			t.NodeItem = nodeitem
			t.Chancount = len(nodes.Data.Topics[i].Channels)
			tlist = append(tlist, t)
		}
		for ii := 0; ii < len(nodes.Data.Topics[i].Channels); ii++ {
			var tc struct {
				ChanName string
			}
			tc.ChanName = nodes.Data.Topics[i].Channels[ii].ChannelName
			nodeTopicItem.Channels = append(nodeTopicItem.Channels, tc)
			var tempTC models.TopicChannel
			tempTC.Channel = tc.ChanName
			tempTC.Topic = nodeTopicItem.TopicName
			topicChan2NodeItemMap.Update(tempTC, nodeitem)
		}

		onenode.Topics = append(onenode.Topics, nodeTopicItem)
	}
	nodemap.Update(nodeitem, onenode)
	return nil
}
