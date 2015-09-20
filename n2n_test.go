package controllers

import (
	"github.com/franela/goreq"
	"log"
	"net/http"
	"strconv"
	"testing"
)

var i = 0

func TestN2N(t *testing.T) {
	go RouteMsg()

	// go func() {
	// 	mux := http.NewServeMux()
	// 	mux.HandleFunc("/api/addsource", handleAddSource)
	// 	mux.HandleFunc("/api/addnode", handleAddNode)
	// 	mux.HandleFunc("/api/delnode", handleDelNode)
	// 	mux.HandleFunc("/api/deltopic", handleDelTopic)
	// 	mux.HandleFunc("/api/addtopic", handleAddTopic)
	// 	mux.HandleFunc("/api/ping", ping)
	// 	http.ListenAndServe("0.0.0.0:3003", mux)
	// }()
	PostMsg(" The Number is :"+strconv.Itoa(i), "testn2n", "testn2n")
	i++
	err := AddSource("localhost:4150", "testn2n", "testn2n")
	if err != nil {
		t.Error(err.Error())
	}
	PostMsg(" The Number is :"+strconv.Itoa(i), "testn2n", "testn2n")
	i++
	PostMsg(" The Number is :"+strconv.Itoa(i), "testn2n", "testn2n")
	i++

}

func PostMsg(body string, topic string, channel string) error {
	res, err := goreq.Request{
		Method: "POST",
		Uri:    "http://localhost:4151/pub?topic=" + topic,
		Body:   body,
	}.Do()
	log.Println(res.Body.ToString())
	return err
}
