package models

type ConfigModel struct {
	NsqdAddr        string
	HttpAddr        string
	LookupdAddr     string
	MasterTopic     string
	TopicMaxChannel int
}
