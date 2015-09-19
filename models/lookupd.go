package models

type Node_lookup struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}
type NodeInfoObj_lookup struct {
	Producers []Node_lookup `json:"producers"`
}
type NodeInfo_lookup struct {
	Status_code int                `json:"status_code"`
	Status_txt  string             `json:"status_txt"`
	Data        NodeInfoObj_lookup `json:"data"`
}
