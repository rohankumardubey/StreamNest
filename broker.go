package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Broker struct {
	mu      sync.Mutex
	topics  map[string][]string   // topic name -> messages
	logDir  string                // directory for topic logs
}

func NewBroker(logDir string) (*Broker, error) {
	os.MkdirAll(logDir, 0755)
	return &Broker{topics: make(map[string][]string), logDir: logDir}, nil
}

// Appends message to topic and saves to file
func (b *Broker) produceHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic   string `json:"topic"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.Topic == "" {
		http.Error(w, "topic required", 400)
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	offset := len(b.topics[req.Topic])
	b.topics[req.Topic] = append(b.topics[req.Topic], req.Message)
	// Append to topic log file
	file, err := os.OpenFile(b.logDir+"/"+req.Topic+".log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err == nil {
		file.WriteString(req.Message + "\n")
		file.Close()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"offset": offset})
}

// Long poll for new messages in topic
func (b *Broker) consumeStreamHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	offsetStr := r.URL.Query().Get("offset")
	offset, err := strconv.Atoi(offsetStr)
	if topic == "" || err != nil || offset < 0 {
		http.Error(w, "invalid topic or offset", 400)
		return
	}
	// Long polling loop: wait up to 10s for new message
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			http.Error(w, "timeout", 408)
			return
		case <-ticker.C:
			b.mu.Lock()
			if msgs, ok := b.topics[topic]; ok && offset < len(msgs) {
				msg := msgs[offset]
				b.mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"offset":  offset,
					"message": msg,
				})
				return
			}
			b.mu.Unlock()
		}
	}
}

func main() {
	broker, err := NewBroker("logs")
	if err != nil {
		panic(err)
	}
	http.HandleFunc("/produce", broker.produceHandler)
	http.HandleFunc("/consume/stream", broker.consumeStreamHandler)
	fmt.Println("Broker running on :8080")
	http.ListenAndServe(":8080", nil)
}
