package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// broker
type Broker struct {
	mu     sync.Mutex
	topics map[string][]string
	logDir string
}

func NewBroker(logDir string) (*Broker, error) {
	os.MkdirAll(logDir, 0755)
	b := &Broker{topics: make(map[string][]string), logDir: logDir}
	// Load messages from logs (optional, for persistence across restarts)
	files, _ := os.ReadDir(logDir)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		topic := strings.TrimSuffix(f.Name(), ".log")
		fp, err := os.Open(logDir + "/" + f.Name())
		if err != nil {
			continue
		}
		scan := bufio.NewScanner(fp)
		for scan.Scan() {
			b.topics[topic] = append(b.topics[topic], scan.Text())
		}
		fp.Close()
	}
	return b, nil
}

// Handles POST /produce
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
	offset := len(b.topics[req.Topic])
	b.topics[req.Topic] = append(b.topics[req.Topic], req.Message)
	b.mu.Unlock()
	// Append to file
	file, err := os.OpenFile(b.logDir+"/"+req.Topic+".log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err == nil {
		file.WriteString(req.Message + "\n")
		file.Close()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"offset": offset})
}

// Handles GET /consume/stream
func (b *Broker) consumeStreamHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	offsetStr := r.URL.Query().Get("offset")
	offset, err := strconv.Atoi(offsetStr)
	if topic == "" || err != nil || offset < 0 {
		http.Error(w, "invalid topic or offset", 400)
		return
	}
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			http.Error(w, "timeout", http.StatusRequestTimeout)
			return
		case <-ticker.C:
			b.mu.Lock()
			msgs := b.topics[topic]
			b.mu.Unlock()
			if offset < len(msgs) {
				msg := msgs[offset]
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"offset":  offset,
					"message": msg,
				})
				return
			}
		}
	}
}

func runBroker() {
	// killing any old instance on port 8080
	killPort8080()
	broker, err := NewBroker("logs")
	if err != nil {
		fmt.Println("Failed to start broker:", err)
		return
	}
	http.HandleFunc("/produce", broker.produceHandler)
	http.HandleFunc("/consume/stream", broker.consumeStreamHandler)
	fmt.Println("Broker running on :8080")
	fmt.Println("Press Ctrl+C to stop.")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Failed to start HTTP server:", err)
	}
}

// Producer
func runProducer() {
	fmt.Print("Enter topic: ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}
	topic := strings.TrimSpace(scanner.Text())
	if topic == "" {
		fmt.Println("No topic specified, exiting.")
		return
	}
	fmt.Printf("Producer for topic '%s' - type messages, Enter to send, 'exit' to quit.\n", topic)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		if text == "exit" {
			break
		}
		reqBody, _ := json.Marshal(map[string]string{"topic": topic, "message": text})
		resp, err := http.Post("http://localhost:8080/produce", "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		var respData map[string]int
		json.NewDecoder(resp.Body).Decode(&respData)
		fmt.Println("Produced, offset:", respData["offset"])
		resp.Body.Close()
	}
	fmt.Println("Producer exited.")
}

// consumer
func runConsumer() {
	fmt.Print("Enter topic to subscribe: ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}
	topic := strings.TrimSpace(scanner.Text())
	if topic == "" {
		fmt.Println("No topic specified, exiting.")
		return
	}
	offset := 0
	fmt.Printf("Consumer for topic '%s' - Waiting for messages...\n", topic)
	for {
		resp, err := http.Get(fmt.Sprintf("http://localhost:8080/consume/stream?topic=%s&offset=%d", topic, offset))
		if err != nil {
			fmt.Println("Error:", err)
			time.Sleep(time.Second)
			continue
		}
		if resp.StatusCode != 200 {
			if resp.StatusCode == 408 {
				resp.Body.Close()
				continue
			}
			b, _ := io.ReadAll(resp.Body)
			fmt.Println("Broker error:", string(b))
			resp.Body.Close()
			time.Sleep(time.Second)
			continue
		}
		var data struct {
			Offset  int    `json:"offset"`
			Message string `json:"message"`
		}
		err = json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()
		if err != nil {
			fmt.Println("Decode error:", err)
			time.Sleep(time.Second)
			continue
		}
		fmt.Printf("[Offset %d] %s\n", data.Offset, data.Message)
		offset++
	}
}

func killPort8080() {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin", "linux":
		fmt.Println("Killing any existing process on port 8080...")
		cmd = exec.Command("bash", "-c", "lsof -ti :8080 | xargs kill -9")
	case "windows":
		fmt.Println("Killing any existing process on port 8080 (Windows)...")
		// This will kill the first process using port 8080 on Windows
		cmd = exec.Command("powershell", "-Command", "Get-Process -Id (Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue | Select-Object -First 1).OwningProcess | Stop-Process -Force")
	default:
		fmt.Println("OS not recognized; skipping port kill.")
		return
	}

	cmd.Run() // Ignore errors if nothing is running
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  go run kafka-lite.go broker     # Start broker server")
		fmt.Println("  go run kafka-lite.go producer   # Start interactive producer")
		fmt.Println("  go run kafka-lite.go consumer   # Start streaming consumer")
		return
	}
	mode := os.Args[1]
	switch mode {
	case "broker":
		runBroker()
	case "producer":
		runProducer()
	case "consumer":
		runConsumer()
	default:
		fmt.Println("Unknown mode:", mode)
	}
}
