# kafka-lite

[![Go](https://img.shields.io/badge/Go-1.19+-00ADD8?logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> 🚀 A minimal, Kafka-inspired clustered message streaming system in Go.
> Spin up multiple brokers, create topics with partitions, and produce/consume messages routed to the correct broker—all via HTTP and a single binary.

---

## ✨ Features

- **Multi-Broker Clustering**
  Start any number of brokers; each knows about its peers and partitions are round-robin–assigned.

- **Partitioned Topics**
  Create a topic with _N_ partitions; each partition has an exact owner broker.

- **Synchronous Topic Propagation**
  `/create-topic` computes the owner list once and propagates it to all brokers before returning.

- **Produce & Consume APIs**
  - `POST /produce` to append a message to a specific topic+partition
  - `GET /consume` to fetch by offset (with automatic forwarding)

- **Metadata Endpoints**
  - `GET /metadata` to discover topic → partition → broker mapping
  - `GET /list-topics` to list all registered topics

- **Interactive CLI Clients**
  Built-in `producer` and `consumer` modes prompt for topic/partition and stream messages.

---

## 🚀 Quickstart

### Prerequisites

- Go 1.19 or higher

### Clone & Build

```sh
git clone https://github.com/rohankumardubey/kafka-lite-cluster.git
cd kafka-lite-cluster
go mod init kafka-lite-cluster
go mod tidy
go build -o kafka-lite-cluster kafka-lite-cluster.go
```

### 1. Start Three Brokers

_Open three terminals and run:_

```sh
# Terminal 1
./kafka-lite-cluster broker --id=1 --port=8080 --peers=localhost:8081,localhost:8082

# Terminal 2
./kafka-lite-cluster broker --id=2 --port=8081 --peers=localhost:8080,localhost:8082

# Terminal 3
./kafka-lite-cluster broker --id=3 --port=8082 --peers=localhost:8080,localhost:8081
```

Each will print:
```
Broker <ID> running on :<port>
```

### 2. Create a Topic

_Send a single request to any broker (e.g. port 8080):_

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{"topic":"test","partitions":3}' \
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
    "test": [
      {"partition":0,"broker":"localhost:8080"},
      {"partition":1,"broker":"localhost:8081"},
      {"partition":2,"broker":"localhost:8082"}
    ]
  }
}
```

### 5. Produce Messages

```sh
./kafka-lite-cluster producer --meta=localhost:8080
```

```
Enter topic: test
Partitions:
  0 on localhost:8080
  1 on localhost:8081
  2 on localhost:8082
Partition?> 1
Type messages (or 'exit'):
> Hello World
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
Enter topic: test
Partitions:
  0 on localhost:8080
  1 on localhost:8081
  2 on localhost:8082
Partition?> 1
[Offset 0] Hello World
[Offset 1] Another message
```

---

## 🏗️ Roadmap

- **Disk Persistence** – survive broker restarts
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
