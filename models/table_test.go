package models

import (
	"log"
	"testing"
	"time"
)

func TestTable(t *testing.T) {
	var table = NewTable()
	var s = struct {
		Topic   string
		Channel string
	}{
		"hello",
		"world",
	}
	log.Println(table.Get(s))
	// var b = []struct {
	// 	Ip       string
	// 	Httpport int
	// 	Tcpport  int
	// }{
	// 	{
	// 		"localhost",
	// 		4150,
	// 		4151,
	// 	},
	// }
	var b = []NodeItem{
		{
			"localhost",
			4150,
			4151,
		},
	}
	go func() {
		table.Update(s, b)
	}()
	time.Sleep(time.Second * 1)
	var r = table.Get(s)
	d, _ := r.([]NodeItem)
	//log.Println(r.(type))
	// if err != false {
	// 	t.Error(err)
	// }
	// for _, val := range r {
	// 	log.Println(val)
	// 	//log.Println([]NodeItem(table.Get(s))[0].Ip)
	// }
	log.Println(d[0].Ip)
	//t.Log(d))
}
