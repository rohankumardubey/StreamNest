package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"github.com/xeipuuv/gojsonschema"
)

// Helper: Marshal to JSON
func MustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

// Round-robin assignment for topic partitions across brokers
func AssignOwners(brokers []string, numPartitions int) []string {
	owners := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		owners[i] = brokers[i%len(brokers)]
	}
	return owners
}

// HTTP handler: create topic (external API)
func (b *Broker) CreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	if req.Topic == "" || req.NumPartitions <= 0 {
		http.Error(w, "topic+positive partitions required", 400)
		return
	}
	all := append([]string{b.Address}, b.Peers...)
	owners := AssignOwners(all, req.NumPartitions)
	b.CreateTopicWithOwners(req.Topic, owners)
	fmt.Printf("[Broker %d] Created topic '%s' owners=%v\n", b.ID, req.Topic, owners)
	// Propagate to peers
	prop := CreateTopicReq{Topic: req.Topic, Owners: owners}
	body := MustJSON(prop)
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

// HTTP handler: create topic (internal propagation)
func (b *Broker) InternalCreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicReq
	json.NewDecoder(r.Body).Decode(&req)
	if req.Topic == "" || len(req.Owners) == 0 {
		w.WriteHeader(400)
		return
	}
	b.CreateTopicWithOwners(req.Topic, req.Owners)
	fmt.Printf("[Broker %d] (internal) Created topic '%s' owners=%v\n", b.ID, req.Topic, req.Owners)
	w.WriteHeader(200)
}

// Assign topic/partitions to in-memory maps, and load persisted logs
func (b *Broker) CreateTopicWithOwners(topic string, owners []string) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	if _, exists := b.Topics[topic]; exists {
		return
	}
	b.Ownership[topic] = owners
	partitions := make([][]string, len(owners))
	for i := range partitions {
		if owners[i] == b.Address {
			msgs, err := LoadPartitionLog(topic, i)
			if err != nil {
				fmt.Printf("[Broker %d] Failed to load partition log: %v\n", b.ID, err)
				partitions[i] = []string{}
			} else {
				partitions[i] = msgs
			}
		} else {
			partitions[i] = []string{}
		}
	}
	b.Topics[topic] = partitions
}

// HTTP handler: expose topic/partition ownership (for clients)
func (b *Broker) MetadataHandler(w http.ResponseWriter, r *http.Request) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	out := MetadataResponse{Topics: make(map[string]TopicMetadata)}
	for topic, owners := range b.Ownership {
		var parts []PartitionInfo
		for i, o := range owners {
			parts = append(parts, PartitionInfo{i, o})
		}
		out.Topics[topic] = TopicMetadata{parts}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// HTTP handler: list topics
func (b *Broker) ListTopicsHandler(w http.ResponseWriter, r *http.Request) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	var names []string
	for t := range b.Ownership {
		names = append(names, t)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]string{"topics": names})
}

// HTTP handler: produce message to a partition (forwards if not owner)
func (b *Broker) ProduceHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid", 400)
		return
	}
	b.Mu.Lock()
	owners, ok := b.Ownership[req.Topic]
	b.Mu.Unlock()
	if !ok || req.Partition < 0 || req.Partition >= len(owners) {
		http.Error(w, "unknown topic/partition", 404)
		return
	}
	owner := owners[req.Partition]
	if owner != b.Address {
		resp, err := http.Post("http://"+owner+"/produce", "application/json", bytes.NewBuffer(MustJSON(req)))
		if err != nil {
			http.Error(w, "forward fail", 500)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Schema validation if exists
	b.Mu.Lock()
	schema, hasSchema := b.Schemas[req.Topic]
	b.Mu.Unlock()
	if hasSchema {
		var parsed interface{}
		if err := json.Unmarshal([]byte(req.Message), &parsed); err != nil {
			http.Error(w, "message is not valid JSON for schema validation", 400)
			return
		}
		result, err := schema.Validate(gojsonschema.NewGoLoader(parsed))
		if err != nil {
			http.Error(w, "schema validation error: "+err.Error(), 400)
			return
		}
		if !result.Valid() {
			http.Error(w, "schema validation failed: "+fmt.Sprint(result.Errors()), 400)
			return
		}
	}
	b.Mu.Lock()
	slice := &b.Topics[req.Topic][req.Partition]
	*slice = append(*slice, req.Message)
	offset := len(*slice) - 1
	b.Mu.Unlock()
	if err := AppendPartitionLog(req.Topic, req.Partition, req.Message); err != nil {
		fmt.Printf("[Broker %d] Error writing log: %v\n", b.ID, err)
	}
	fmt.Printf("[Broker %d] + topic=%s p=%d off=%d\n", b.ID, req.Topic, req.Partition, offset)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"offset": offset})
}

// HTTP handler: consume message from a partition/offset (forwards if not owner)
func (b *Broker) ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	part, _ := strconv.Atoi(r.URL.Query().Get("partition"))
	off, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	b.Mu.Lock()
	owners, ok := b.Ownership[topic]
	b.Mu.Unlock()
	if !ok || part < 0 || part >= len(owners) {
		http.Error(w, "unknown topic/partition", 404)
		return
	}
	owner := owners[part]
	if owner != b.Address {
		url := fmt.Sprintf("http://%s/consume?topic=%s&partition=%d&offset=%d", owner, topic, part, off)
		resp, err := http.Get(url)
		if err != nil {
			http.Error(w, "forward fail", 500)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
	b.Mu.Lock()
	msgs := b.Topics[topic][part]
	b.Mu.Unlock()
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

// SCHEMA REGISTRY HANDLER
func (b *Broker) RegisterSchemaHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic  string                 `json:"topic"`
		Schema map[string]interface{} `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid", 400)
		return
	}
	if req.Topic == "" || req.Schema == nil {
		http.Error(w, "topic and schema required", 400)
		return
	}
	schemaLoader := gojsonschema.NewGoLoader(req.Schema)
	compiled, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		http.Error(w, "schema compilation error: "+err.Error(), 400)
		return
	}
	b.Mu.Lock()
	b.Schemas[req.Topic] = compiled
	b.Mu.Unlock()
	// Persist schema to disk
	if err := SaveSchema(req.Topic, req.Schema); err != nil {
		http.Error(w, "failed to persist schema: "+err.Error(), 500)
		return
	}
	w.WriteHeader(200)
	w.Write([]byte(`{"status":"schema registered"}`))
}

// Broker constructor
func NewBroker(id, port int, peers []string) *Broker {
	addr := fmt.Sprintf("localhost:%d", port)
	return &Broker{
		ID:        id,
		Address:   addr,
		Peers:     peers,
		Port:      port,
		Topics:    make(map[string][][]string),
		Ownership: make(map[string][]string),
		Schemas:   make(map[string]*gojsonschema.Schema),
	}
}

// Main broker server
func RunBroker(id, port int, peers []string) {

	b := NewBroker(id, port, peers)

	// Load schemas from disk
	schemaMap, err := LoadAllSchemas()
	if err == nil {
		for topic, schemaObj := range schemaMap {
			schemaLoader := gojsonschema.NewGoLoader(schemaObj)
			compiled, err := gojsonschema.NewSchema(schemaLoader)
			if err == nil {
				b.Schemas[topic] = compiled
			}
		}
	}

	http.HandleFunc("/register-schema", b.RegisterSchemaHandler)
	http.HandleFunc("/create-topic", b.CreateTopicHandler)
	http.HandleFunc("/internal-create-topic", b.InternalCreateTopicHandler)
	http.HandleFunc("/metadata", b.MetadataHandler)
	http.HandleFunc("/list-topics", b.ListTopicsHandler)
	http.HandleFunc("/produce", b.ProduceHandler)
	http.HandleFunc("/consume", b.ConsumeHandler)
	fmt.Printf("Broker %d running on :%d\n", id, port)
	fmt.Println("=========================================")
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
