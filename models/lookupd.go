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

type NodeInfo_nsqd struct {
	Status_code int            `json:"status_code"`
	Status_txt  string         `json:"status_txt"`
	Data        StatsInfo_nsqd `json:"data"`
}

type StatsInfo_nsqd struct {
	Version   string       `json:"version"`
	Health    string       `json:"health"`
	StartTime int64        `json:"start_time"`
	Topics    []TopicStats `json:"topics"`
}

type TopicStats struct {
	TopicName    string         `json:"topic_name"`
	Channels     []ChannelStats `json:"channels"`
	Depth        int64          `json:"depth"`
	BackendDepth int64          `json:"backend_depth"`
	MessageCount uint64         `json:"message_count"`
	Paused       bool           `json:"paused"`

	//E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

type ChannelStats struct {
	ChannelName   string `json:"channel_name"`
	Depth         int64  `json:"depth"`
	BackendDepth  int64  `json:"backend_depth"`
	InFlightCount int    `json:"in_flight_count"`
	DeferredCount int    `json:"deferred_count"`
	MessageCount  uint64 `json:"message_count"`
	RequeueCount  uint64 `json:"requeue_count"`
	TimeoutCount  uint64 `json:"timeout_count"`
	//Clients       []ClientStats `json:"clients"`
	Paused bool `json:"paused"`

	//	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}
