package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
	"kafka-lite/internal/broker"
	"kafka-lite/internal/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  broker   --id=1 --port=8080 --peers=a,b [--count=N]")
		fmt.Println("  producer --meta=host:port")
		fmt.Println("  consumer --meta=host:port")
		return
	}
	switch os.Args[1] {
	case "broker":
		fs := flag.NewFlagSet("broker", flag.ExitOnError)
		id := fs.Int("id", 1, "broker id")
		port := fs.Int("port", 8080, "port")
		peers := fs.String("peers", "", "comma sep peers")
		count := fs.Int("count", 0, "number of brokers to start")
		bin := fs.String("bin", os.Args[0], "binary path (for self-spawn)")
		fs.Parse(os.Args[2:])

		if *count > 1 {
			var allPeers []string
			for i := 0; i < *count; i++ {
				allPeers = append(allPeers, fmt.Sprintf("localhost:%d", 8080+i))
			}
			for i := 0; i < *count; i++ {
				id := i + 1
				port := 8080 + i
				var peers []string
				for j, addr := range allPeers {
					if j != i {
						peers = append(peers, addr)
					}
				}
				peerArg := strings.Join(peers, ",")
				cmd := exec.Command(*bin,
					"broker",
					fmt.Sprintf("--id=%d", id),
					fmt.Sprintf("--port=%d", port),
					fmt.Sprintf("--peers=%s", peerArg),
				)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				fmt.Printf("Starting broker %d on port %d with peers: %s\n", id, port, peerArg)
				if err := cmd.Start(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to start broker %d: %v\n", id, err)
					continue
				}
				time.Sleep(500 * time.Millisecond)
			}
			for {
				time.Sleep(time.Hour)
			}
		} else {
			peerList := []string{}
			if *peers != "" {
				peerList = strings.Split(*peers, ",")
			}
			broker.RunBroker(*id, *port, peerList)
		}

	case "producer":
		fs := flag.NewFlagSet("producer", flag.ExitOnError)
		meta := fs.String("meta", "localhost:8080", "metadata endpoint")
		fs.Parse(os.Args[2:])
		client.RunProducer(*meta)

	case "consumer":
		fs := flag.NewFlagSet("consumer", flag.ExitOnError)
		meta := fs.String("meta", "localhost:8080", "metadata endpoint")
		fs.Parse(os.Args[2:])
		client.RunConsumer(*meta)

	default:
		fmt.Println("Unknown mode")
	}
}
