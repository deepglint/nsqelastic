package models

import (
	"sync"
)

type BigTable struct {
	mutex *sync.Mutex
	table *map[interface{}]interface{}
}

func NewTable() *BigTable {
	t := new(BigTable)
	t.mutex = &sync.Mutex{}
	t.table = new(map[interface{}]interface{})
	return t
}

func (this *BigTable) Update(key interface{}, val interface{}) {
	this.mutex.Lock()
	if (*this.table)[key] != nil {
		(*this.table)[key] = val
	}
	this.mutex.Unlock()
}

func (this *BigTable) Get(key interface{}) interface{} {
	var tmp interface{}
	this.mutex.Lock()
	tmp = (*this.table)[key]
	this.mutex.Unlock()
	return tmp
}
