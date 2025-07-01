# kafka-lite

[![Go](https://img.shields.io/badge/Go-1.19+-00ADD8?logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> 🚀 A minimal, Kafka-inspired clustered message streaming system in Go.
> Launch a broker cluster with any number of brokers, create topics with any number of partitions, and interactively produce and consume messages—partition ownership is distributed across brokers just like in real Kafka!

---

## ✨ Features

- **Multi-Broker Clustering:** Start N brokers at once, each automatically aware of peers.
- **Any Partition Count:** Create topics with any number of partitions, independently of broker count.
- **Round-Robin Assignment:** Partitions are spread evenly across brokers (round-robin).
- **HTTP APIs:** Create topics, list topics, produce to and consume from any partition over HTTP.
- **CLI Producer & Consumer:** Simple interactive clients for message publishing and consumption.
- **Persistent Logs:** Each partition's data is stored on disk and survives restarts.
- **Modular Go Code:** Idiomatic structure using `/cmd` and `/internal` directories.

---

## 🚀 Quickstart

### Prerequisites

- Go 1.19 or higher

### Clone & Build

```sh
git clone https://github.com/rohankumardubey/kafka-lite.git
cd kafka-lite
go mod tidy
go build -o kafka-lite-cluster ./cmd/kafka-lite-cluster
```

### 1. Start Three Brokers

_Run this in one terminal (spawns all brokers as subprocesses):_

```sh
# Terminal 1
./kafka-lite-cluster broker --count=3
```

Each will print:
```
Starting broker 1 on port 8080 with peers: localhost:8081,localhost:8082
Broker 1 running on :8080
Starting broker 2 on port 8081 with peers: localhost:8080,localhost:8082
Broker 2 running on :8081
Starting broker 3 on port 8082 with peers: localhost:8080,localhost:8081
Broker 3 running on :8082

```

### 2. Create a Topic (with Any Partition Count!)

_In a second terminal, run:_

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{"topic":"demo","partitions":7}' \
  http://localhost:8080/create-topic
```

_Response:_
```json
{"status":"created"}
```

### 3. List Topics

```sh
curl http://localhost:8080/list-topics
```

_Response:_
```json
{"topics":["test"]}
```

### 4. View Cluster Metadata

```sh
curl http://localhost:8080/metadata
```

_Response Example:_
```json
{
  "topic_partitions": {
    "demo": {
      "partitions": [
        {"partition":0,"broker":"localhost:8080"},
        {"partition":1,"broker":"localhost:8081"},
        {"partition":2,"broker":"localhost:8082"},
        {"partition":3,"broker":"localhost:8080"},
        {"partition":4,"broker":"localhost:8081"},
        {"partition":5,"broker":"localhost:8082"},
        {"partition":6,"broker":"localhost:8080"}
      ]
    }
  }
}
```

### 5. Produce Messages

```sh
./kafka-lite-cluster producer --meta=localhost:8080
```

```
Enter topic: demo
Partitions:
  0 on localhost:8080
  1 on localhost:8081
  2 on localhost:8082
  3 on localhost:8080
  4 on localhost:8081
  5 on localhost:8082
  6 on localhost:8080
Partition?> 4
Type messages (or 'exit'):
> Hello Kafka-lite
offset: 0
> Another message
offset: 1
> exit
```

### 6. Consume Messages

```sh
./kafka-lite-cluster consumer --meta=localhost:8080
```
```
Enter topic: demo
Partitions:
  0 on localhost:8080
  1 on localhost:8081
  2 on localhost:8082
  3 on localhost:8080
  4 on localhost:8081
  5 on localhost:8082
  6 on localhost:8080
Partition?> 4
[Offset 0] Hello Kafka-lite
[Offset 1] Another message
```

---

## 📁 Project Layout
```sh
kafka-lite/
├── cmd/
│   └── kafka-lite-cluster/
│       └── main.go
├── internal/
│   ├── broker/
│   │   ├── types.go
│   │   ├── storage.go
│   │   └── broker.go
│   └── client/
│       └── client.go
├── data/          # runtime logs: <topic>_<partition>.log
├── go.mod
└── README.md
```

---

## 🏗️ Roadmap

- **Disk Persistence** – Disk persistence metadata (for seamless restarts)
- **Replication & Failover** – mirror partitions across brokers
- **Consumer Groups** – manage offsets per group
- **Docker Compose** – launch cluster with a single command
- **Metrics & Monitoring** – Prometheus endpoints

---

## 🤝 Contributing

1. Fork the repo
2. Create a branch:
   ```sh
   git checkout -b my-feature
   ```
3. Commit your changes:
   ```sh
   git commit -am 'Add cool feature'
   ```
4. Push to your branch:
   ```sh
   git push origin my-feature
   ```
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
