package controllers

import (
	"encoding/json"
	"fmt"
	"github.com/deepglint/nsqelastic/models"
	"io/ioutil"
	"net/http"
	"strings"
)

type NsqController struct {
	Config                *models.ConfigModel
	TopicChan2NodeItemMap *models.BigTable
	Topic2TopicItemMap    *models.BigTable
	NodeItem2NodeMap      *models.BigTable
}

func NewNsqController(config *models.ConfigModel, topicChan2NodeItemMap, topic2TopicItemMap, nodeItem2NodeMap *models.BigTable) *NsqController {
	c := &NsqController{
		Config:                config,
		TopicChan2NodeItemMap: topicChan2NodeItemMap,
		Topic2TopicItemMap:    topic2TopicItemMap,
		NodeItem2NodeMap:      nodeItem2NodeMap,
	}
	return c
}

func (n *NsqController) Pub(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	//fmt.Printf("body%s %s\n", r.Method, string(body))
	r.ParseForm()
	//fmt.Printf("Pub %s\n", fmt.Sprintf("http://%s/pub?topic=%s", n.Config.NsqdAddr, strings.Join(r.Form["topic"], "")))

	resp, err := http.Post(fmt.Sprintf("http://%s/pub?topic=%s", n.Config.NsqdAddr, n.Config.MasterTopic),
		"application/x-www-form-urlencoded",
		strings.NewReader(strings.Join(r.Form["topic"], "")+"\n"+string(body)))
	if err != nil {
		fmt.Println(err)
	}

	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
	}
	fmt.Fprint(w, fmt.Sprintf("%s", string(body)))
}
func (n *NsqController) Sub(w http.ResponseWriter, r *http.Request) {
	//mapNTC := GetMapNTC()
	var topic, channel string
	if v, ok := r.Form["topic"]; ok {
		topic = strings.Join(v, "")
	} else {
		fmt.Fprint(w, fmt.Sprintf("%s", "topic error"))
		return
	}
	if v, ok := r.Form["channel"]; ok {
		channel = strings.Join(v, "")
	} else {
		fmt.Fprint(w, fmt.Sprintf("%s", "channel error"))
		return
	}
	node, err := n.TopicChan2NodeItemMap.GetNodeItem(topic, channel)
	if err != nil {
		fmt.Fprint(w, fmt.Sprintf("%s", err))
		return
	}
	if node != nil {
		jsonstr, _ := json.Marshal(node)
		fmt.Fprint(w, fmt.Sprintf("%s", jsonstr))
	}
	//check topic-node list if==nil or not all full select one almost full; if all is full, create new node;
	topicitems, _err := n.TopicChan2NodeItemMap.GetTopicItem(topic)
	if _err != nil {
		fmt.Fprint(w, fmt.Sprintf("%s", _err))
		return
	}
	if topicitems != nil && n.isAllFull(topicitems) {
		//todo:create new one
		//jsonstr, _ := json.Marshal(topicitems)
		fmt.Fprint(w, "create new one")
		return
	} else {
		node := n.selectMaxOne(topicitems)
		jsonstr, _ := json.Marshal(node)
		fmt.Fprint(w, fmt.Sprintf("%s", jsonstr))
		return
	}
	return
}

func (n *NsqController) isAllFull(items []models.TopicItem) bool {
	for i := 0; i < len(items); i++ {
		if items[i].Chancount < n.Config.TopicMaxChannel {
			return false
		}
	}
	return true
}

func (n *NsqController) selectMaxOne(items []models.TopicItem) models.NodeItem {
	max_index := 0
	for i := 0; i < len(items); i++ {
		if items[i].Chancount < n.Config.TopicMaxChannel && items[i].Chancount >= items[max_index].Chancount {
			max_index = i
		}
	}
	return items[max_index].NodeItem
}
