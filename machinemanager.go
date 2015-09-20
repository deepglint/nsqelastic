package main

import (
	"encoding/json"
	"github.com/deepglint/nsqelastic/models"
	//"log"
	"net/http"
)

var exitchan = make(chan bool)

var machines = []models.NodeItem{
	{
		Ip:       "localhost",
		Tcpport:  4150,
		Httpport: 4151,
	},
	{
		Ip:       "192.168.251.113",
		Tcpport:  4150,
		Httpport: 4151,
	},
}

func handleGetAll(w http.ResponseWriter, r *http.Request) {
	body, err := json.Marshal(machines)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(body)
}

func handleAdd(w http.ResponseWriter, r *http.Request) {
	//body, err := json.Marshal(v)
	//log.Println("here")
	w.Write([]byte("error"))
}

func main() {
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/getall", handleGetAll)
		mux.HandleFunc("/api/add", handleAdd)
		//mux.HandleFunc("/api/addnode", handleAddNode)
		http.ListenAndServe("0.0.0.0:3004", mux)
	}()
	<-exitchan
}
