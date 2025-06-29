package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

func main() {
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
