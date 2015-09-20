package models

import (
	//"log"
	"errors"
	"sync"
)

type TopicChannel struct {
	Topic   string
	Channel string
}

type NodeItem struct {
	Ip       string
	Httpport int
	Tcpport  int
}

func (n *NodeItem) Eq(t NodeItem) bool {
	if n.Ip == t.Ip && (n.Httpport == t.Httpport || n.Tcpport == t.Tcpport) {
		return true
	}
	return false
}

type TopicItem struct {
	TopicName string
	NodeItem
	Chancount int
}

const (
	UNUSE string = "unuse"
	DEL   string = "delete"
	USING string = "using"
)

type Node struct {
	NodeItem
	//Stats  string //"delete"
	Topics []struct {
		TopicName string
		//	Stats     string
		Channels []struct {
			//Stats    string
			ChanName string
		}
	}
}

type NodeState struct {
	NodeItem
	Stats string //"unuse" ""
}

type BigTable struct {
	mutex *sync.Mutex
	Table map[interface{}]interface{}
}

func NewTable() *BigTable {
	t := new(BigTable)
	t.mutex = &sync.Mutex{}
	t.Table = make(map[interface{}]interface{})
	return t
}

func (this *BigTable) Update(key interface{}, val interface{}) {
	this.mutex.Lock()
	//if (*this.table)[key] != nil {
	(this.Table)[key] = val
	//}
	this.mutex.Unlock()
}

func (this *BigTable) Get(key interface{}) interface{} {
	var tmp interface{}
	this.mutex.Lock()
	tmp = (this.Table)[key]
	this.mutex.Unlock()
	return tmp
}

func (this *BigTable) Replace(other *BigTable) {
	this.mutex.Lock()
	this.Table = other.Table
	this.mutex.Unlock()
}

func (this *BigTable) GetNodeItem(topic, channel string) (*NodeItem, error) {
	key := TopicChannel{
		Topic:   topic,
		Channel: channel,
	}
	nodeInterface := this.Get(key)
	if nodeInterface != nil {
		nodePtr := new(NodeItem)
		node, ok := nodeInterface.(NodeItem)
		if ok {
			*nodePtr = node
			return nodePtr, nil
		} else {
			return nil, errors.New("convert error")
		}

	} else {
		return nil, nil
	}
}

func (this *BigTable) GetTopicItem(topic string) ([]TopicItem, error) {

	itemsInterface := this.Get(topic)
	if itemsInterface != nil {
		items, ok := itemsInterface.([]TopicItem)
		if ok {
			return items, nil
		} else {
			return nil, errors.New("convert error")
		}

	} else {
		return nil, nil
	}
}

func (this *BigTable) GetMaxNodeItem(TopicMaxChannel int) *NodeItem {
	var max *NodeItem
	for k, _ := range this.Table {
		max = new(NodeItem)
		*max, _ = k.(NodeItem)
		break
	}
	return max
}
