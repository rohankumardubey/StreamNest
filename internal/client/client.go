package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"kafka-lite/internal/broker"
)

func RunProducer(meta string) {
	r := bufio.NewReader(os.Stdin)
	fmt.Print("Enter topic: ")
	topic, _ := r.ReadString('\n')
	topic = strings.TrimSpace(topic)

	resp, _ := http.Get("http://" + meta + "/metadata")
	var metaResp broker.MetadataResponse
	json.NewDecoder(resp.Body).Decode(&metaResp)
	resp.Body.Close()

	parts := metaResp.Topics[topic].Partitions
	fmt.Println("Partitions:")
	for _, p := range parts {
		fmt.Printf("  %d on %s\n", p.Partition, p.Broker)
	}
	fmt.Print("Partition?> ")
	pl, _ := r.ReadString('\n')
	part, _ := strconv.Atoi(strings.TrimSpace(pl))

	fmt.Println("Type messages (or 'exit'):")
	for {
		fmt.Print("> ")
		text, _ := r.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "exit" {
			break
		}
		req := map[string]interface{}{"topic": topic, "partition": part, "message": text}
		resp, _ := http.Post("http://"+meta+"/produce", "application/json", bytes.NewBuffer(broker.MustJSON(req)))
		var out map[string]int
		json.NewDecoder(resp.Body).Decode(&out)
		fmt.Println("offset:", out["offset"])
		resp.Body.Close()
	}
}

func RunConsumer(meta string) {
	r := bufio.NewReader(os.Stdin)
	fmt.Print("Enter topic: ")
	topic, _ := r.ReadString('\n')
	topic = strings.TrimSpace(topic)

	resp, _ := http.Get("http://" + meta + "/metadata")
	var metaResp broker.MetadataResponse
	json.NewDecoder(resp.Body).Decode(&metaResp)
	resp.Body.Close()

	parts := metaResp.Topics[topic].Partitions
	fmt.Println("Partitions:")
	for _, p := range parts {
		fmt.Printf("  %d on %s\n", p.Partition, p.Broker)
	}
	fmt.Print("Partition?> ")
	pl, _ := r.ReadString('\n')
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
			time.Sleep(time.Second)
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
