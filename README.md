# StreamNest

[![Go](https://img.shields.io/badge/Go-1.19+-00ADD8?logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

<p align="center">
  <img src="https://github.com/user-attachments/assets/77feb735-0232-40b6-a7f1-81a82629408d" alt="StreamNest" width="400"/>
</p>

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

---

## 🚀 Quickstart

### Prerequisites

- Go 1.19 or higher

### Clone & Build

```sh
git clone https://github.com/rohankumardubey/StreamNest.git
cd StreamNest
go mod tidy
go build -o stream-nest-cluster ./cmd/stream-nest-cluster
```

### 1. Start Three Brokers

_Run this in one terminal (spawns all brokers as subprocesses):_

```sh
./stream-nest-cluster broker --count=3
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
### 5. Register Schema

_Register a schema for a topic:_
```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "topic":"demo",
    "schema":{
      "type":"object",
      "properties":{
        "name":{"type":"string"},
        "age":{"type":"number"}
      },
      "required":["name","age"]
    }
  }' \
  http://localhost:8080/register-schema
```
_**Note: If you are registering a schema in Schema Registry, you must send the messages in the schema defined in schema registry format; otherwise the messages will be ignored by the consumer due to strict Schema Registry validations.If you are unaware of the schema just ignore this step so no validations happen and broker & consumer accepts all messages.**_

### 6. Produce Messages

_**Producing a valid message with the schema from /produce endpoint**_

_(if you plan to specify the partition value manually without key then it will send the message to specified partition)_

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "topic":"demo",
    "partition":0,
    "message":"{\"name\":\"Alice\",\"age\":30}"
  }' \
  http://localhost:8080/produce
```

_(if no partition value is specified and key is given then it will calculate partition based on hash function)_


```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "topic":"demo",
    "key": "id1",
    "message":"{\"name\":\"Alice\",\"age\":30}"
  }' \
  http://localhost:8080/produce
```

_(if no partition value and no key is given then it will follow the round robin strategy)_

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "topic":"demo",
    "message":"{\"name\":\"Alice\",\"age\":30}"
  }' \
  http://localhost:8080/produce
```


_Producing a valid message without schema from CLI_
```sh
./stream-nest-cluster producer --meta=localhost:8080
```
_Response_

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
> Hello
offset: 0
> Another message
offset: 1
> exit
```

### 7. Consume Messages

```sh
./stream-nest-cluster consumer --meta=localhost:8080
```

_Response_
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
[Offset 0] Hello
[Offset 1] Another message
```

---

## 📁 Project Layout
```sh
StreamNest/
├── cmd/
│   └── stream-nest-cluster/
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
