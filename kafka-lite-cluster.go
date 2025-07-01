package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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

// Broker holds in-memory logs and the agreed ownership list for each topic.
type Broker struct {
	ID        int
	Address   string
	Peers     []string
	Port      int
	topics    map[string][][]string // topic → [partition] → messages
	ownership map[string][]string   // topic → owners list (len = #partitions)
	mu        sync.Mutex
}

func NewBroker(id, port int, peers []string) *Broker {
	addr := fmt.Sprintf("localhost:%d", port)
	return &Broker{
		ID:        id,
		Address:   addr,
		Peers:     peers,
		Port:      port,
		topics:    make(map[string][][]string),
		ownership: make(map[string][]string),
	}
}

type createTopicReq struct {
	Topic         string   `json:"topic"`
	NumPartitions int      `json:"partitions"`
	Owners        []string `json:"owners,omitempty"` // populated once by the broker you call
}

// POST /create-topic → computes owners, sets up locally, then propagates.
func (b *Broker) createTopicHandler(w http.ResponseWriter, r *http.Request) {
	var req createTopicReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	if req.Topic == "" || req.NumPartitions <= 0 {
		http.Error(w, "topic + positive partitions required", 400)
		return
	}
	// Compute the owners list exactly once
	all := append([]string{b.Address}, b.Peers...)
	owners := make([]string, req.NumPartitions)
	for i := 0; i < req.NumPartitions; i++ {
		owners[i] = all[i%len(all)]
	}
	// Install locally
	b.createTopicWithOwners(req.Topic, owners)
	fmt.Printf("[Broker %d] Created topic '%s' owners=%v\n", b.ID, req.Topic, owners)

	// Synchronously propagate to peers
	prop := createTopicReq{Topic: req.Topic, Owners: owners}
	body := mustJSON(prop)
	for _, peer := range b.Peers {
		if peer == b.Address {
			continue
		}
		url := "http://" + peer + "/internal-create-topic"
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
		if err != nil {
			fmt.Printf("[Broker %d] Propagate to %s failed: %v\n", b.ID, peer, err)
			continue
		}
		resp.Body.Close()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

// POST /internal-create-topic → used by peers to install the exact same owners.
func (b *Broker) internalCreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	var req createTopicReq
	json.NewDecoder(r.Body).Decode(&req)
	if req.Topic == "" || len(req.Owners) == 0 {
		w.WriteHeader(400)
		return
	}
	b.createTopicWithOwners(req.Topic, req.Owners)
	fmt.Printf("[Broker %d] (internal) Created topic '%s' owners=%v\n", b.ID, req.Topic, req.Owners)
	w.WriteHeader(200)
}

// shared helper to install topic + empty slices
func (b *Broker) createTopicWithOwners(topic string, owners []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ownership[topic] = owners
	partitions := make([][]string, len(owners))
	for i := range partitions {
		partitions[i] = []string{}
	}
	b.topics[topic] = partitions
}

// GET /metadata
func (b *Broker) metadataHandler(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := MetadataResponse{Topics: make(map[string]TopicMetadata)}
	for topic, owners := range b.ownership {
		var parts []PartitionInfo
		for i, o := range owners {
			parts = append(parts, PartitionInfo{i, o})
		}
		out.Topics[topic] = TopicMetadata{parts}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// GET /list-topics
func (b *Broker) listTopicsHandler(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var names []string
	for t := range b.ownership {
		names = append(names, t)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]string{"topics": names})
}

// POST /produce
func (b *Broker) produceHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid", 400)
		return
	}
	b.mu.Lock()
	owners, ok := b.ownership[req.Topic]
	b.mu.Unlock()
	if !ok || req.Partition < 0 || req.Partition >= len(owners) {
		http.Error(w, "unknown topic/partition", 404)
		return
	}
	owner := owners[req.Partition]
	if owner != b.Address {
		// forward, propagating status code
		resp, err := http.Post("http://"+owner+"/produce",
			"application/json",
			bytes.NewBuffer(mustJSON(req)))
		if err != nil {
			http.Error(w, "forward failed", 500)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
	// local append
	b.mu.Lock()
	slice := &b.topics[req.Topic][req.Partition]
	*slice = append(*slice, req.Message)
	offset := len(*slice) - 1
	fmt.Printf("[Broker %d] + topic=%s p=%d off=%d\n", b.ID, req.Topic, req.Partition, offset)
	b.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"offset": offset})
}

// GET /consume
func (b *Broker) consumeHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	part, _ := strconv.Atoi(r.URL.Query().Get("partition"))
	off, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	b.mu.Lock()
	owners, ok := b.ownership[topic]
	b.mu.Unlock()
	if !ok || part < 0 || part >= len(owners) {
		http.Error(w, "unknown topic/partition", 404)
		return
	}
	owner := owners[part]
	if owner != b.Address {
		// forward, propagating status code
		url := fmt.Sprintf("http://%s/consume?topic=%s&partition=%d&offset=%d",
			owner, topic, part, off)
		resp, err := http.Get(url)
		if err != nil {
			http.Error(w, "forward failed", 500)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
	// local read
	b.mu.Lock()
	msgs := b.topics[topic][part]
	b.mu.Unlock()
	if off < 0 || off >= len(msgs) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	fmt.Printf("[Broker %d] - topic=%s p=%d off=%d\n", b.ID, topic, part, off)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"offset":  off,
		"message": msgs[off],
	})
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func runBroker(id, port int, peers []string) {
	b := NewBroker(id, port, peers)
	http.HandleFunc("/create-topic", b.createTopicHandler)
	http.HandleFunc("/internal-create-topic", b.internalCreateTopicHandler)
	http.HandleFunc("/metadata", b.metadataHandler)
	http.HandleFunc("/list-topics", b.listTopicsHandler)
	http.HandleFunc("/produce", b.produceHandler)
	http.HandleFunc("/consume", b.consumeHandler)
	fmt.Printf("Broker %d running on :%d\n", id, port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func runProducer(meta string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter topic: ")
	topic, _ := reader.ReadString('\n')
	topic = strings.TrimSpace(topic)

	// fetch metadata
	resp, _ := http.Get("http://" + meta + "/metadata")
	defer resp.Body.Close()
	var metaResp MetadataResponse
	json.NewDecoder(resp.Body).Decode(&metaResp)

	parts := metaResp.Topics[topic].Partitions
	fmt.Println("Partitions:")
	for _, p := range parts {
		fmt.Printf("  %d on %s\n", p.Partition, p.Broker)
	}
	fmt.Print("Partition?> ")
	pl, _ := reader.ReadString('\n')
	part, _ := strconv.Atoi(strings.TrimSpace(pl))

	fmt.Println("Type messages (or 'exit'):")
	offsets := 0
	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "exit" {
			break
		}
		req := map[string]interface{}{"topic": topic, "partition": part, "message": text}
		resp, _ := http.Post("http://"+meta+"/produce", "application/json", bytes.NewBuffer(mustJSON(req)))
		var out map[string]int
		json.NewDecoder(resp.Body).Decode(&out)
		fmt.Println("offset:", out["offset"])
		resp.Body.Close()
		offsets++
	}
}

func runConsumer(meta string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter topic: ")
	topic, _ := reader.ReadString('\n')
	topic = strings.TrimSpace(topic)

	resp, _ := http.Get("http://" + meta + "/metadata")
	defer resp.Body.Close()
	var metaResp MetadataResponse
	json.NewDecoder(resp.Body).Decode(&metaResp)

	parts := metaResp.Topics[topic].Partitions
	fmt.Println("Partitions:")
	for _, p := range parts {
		fmt.Printf("  %d on %s\n", p.Partition, p.Broker)
	}
	fmt.Print("Partition?> ")
	pl, _ := reader.ReadString('\n')
	part, _ := strconv.Atoi(strings.TrimSpace(pl))

	offset := 0
	for {
		url := fmt.Sprintf("http://%s/consume?topic=%s&partition=%d&offset=%d", meta, topic, part, offset)
		resp, err := http.Get(url)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var data struct {
			Offset  int    `json:"offset"`
			Message string `json:"message"`
		}
		json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()
		fmt.Printf("[Offset %d] %s\n", data.Offset, data.Message)
		offset++
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  broker   --id=1 --port=8080 --peers=host:port,...")
		fmt.Println("  producer --meta=host:port")
		fmt.Println("  consumer --meta=host:port")
		return
	}
	switch os.Args[1] {
	case "broker":
		fs := flag.NewFlagSet("broker", flag.ExitOnError)
		id := fs.Int("id", 1, "broker id")
		port := fs.Int("port", 8080, "port to listen on")
		peers := fs.String("peers", "", "comma-separated peer addresses")
		fs.Parse(os.Args[2:])
		pl := []string{}
		if *peers != "" {
			pl = strings.Split(*peers, ",")
		}
		runBroker(*id, *port, pl)

	case "producer":
		fs := flag.NewFlagSet("producer", flag.ExitOnError)
		meta := fs.String("meta", "localhost:8080", "metadata endpoint")
		fs.Parse(os.Args[2:])
		runProducer(*meta)

	case "consumer":
		fs := flag.NewFlagSet("consumer", flag.ExitOnError)
		meta := fs.String("meta", "localhost:8080", "metadata endpoint")
		fs.Parse(os.Args[2:])
		runConsumer(*meta)

	default:
		fmt.Println("Unknown mode")
	}
}
