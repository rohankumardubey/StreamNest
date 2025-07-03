package broker

import (
	"sync"
	"github.com/xeipuuv/gojsonschema"
)

type PartitionInfo struct {
	Partition int    `json:"partition"`
	Broker    string `json:"broker"`
}

type TopicMetadata struct {
	Partitions []PartitionInfo `json:"partitions"`
}

type MetadataResponse struct {
	Topics map[string]TopicMetadata `json:"topic_partitions"`
}

type Broker struct {
	ID        int
	Address   string
	Peers     []string
	Port      int
	Topics    map[string][][]string
	Ownership map[string][]string
	Schemas   map[string]*gojsonschema.Schema
	RoundRobin map[string]int 					// For round robin per topic
	Mu        sync.Mutex
}

type CreateTopicReq struct {
	Topic         string   `json:"topic"`
	NumPartitions int      `json:"partitions"`
	Owners        []string `json:"owners,omitempty"`
}
