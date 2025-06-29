package main

import (
	"encoding/json"
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
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
