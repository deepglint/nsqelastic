package models

import (
	"log"
	"testing"
	"time"
)

func TestTable(t *testing.T) {
	var table = NewTable()
	var s = struct {
		topic   string
		channel string
	}{
		"hello",
		"world",
	}
	log.Println(table.Get(s))
	var b = []struct {
		ip       string
		httpport int
		tcpport  int
	}{
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
	//    table.Get(s)
	log.Println(table.Get(s))

	t.Log("Hello world")
}
