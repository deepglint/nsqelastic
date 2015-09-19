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

type TopicItem struct {
	TopicName string
	NodeItem
	Chancount int
}

type Node struct {
	NodeItem
	Topics []struct {
		TopicName string
		Channels  []struct {
			ChanName string
		}
	}
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
