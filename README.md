# kafka-lite

[![Go](https://img.shields.io/badge/Go-1.19+-00ADD8?logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> 🚀 A minimal, **Kafka-inspired message streaming system** in Go – standalone binary!

---

## ✨ Features

- **Multiple Topics** – organize messages by topic
- **Streaming Consumer** – see messages live as they arrive
- **Interactive Producer** – send messages in real-time from your CLI
- **Simple Broker** – single binary, persistent log files per topic
- **Tiny Footprint** – all in a single Go file!
- **Zero Dependencies** – only standard library required

---

## 🛠️ Usage

### 1. **Clone & Setup**

```sh
git clone https://github.com/rohankumardubey/kafka-lite.git
cd kafka-lite
go mod init kafka-lite
go mod tidy
```


### 2. **Free Port 8080 (macOS)**
```sh
lsof -ti :8080 | xargs kill -9
```


### 3. **Build a binary**
```sh
go build -o kafka-lite kafka-lite.go
```

### 4. **Start the broker , Producer & consumer in separate terminals**
```sh
./kafka-lite broker
./kafka-lite producer
./kafka-lite consumer
```

### 5. 📝 **Topic Management via API**

**Create a Topic : You can explicitly create a topic with a POST request.**
```sh
curl -X POST -H "Content-Type: application/json" -d '{"topic":"mytopic"}' http://localhost:8080/create-topic
```

**Example response:**
```sh
{"status":"created"}
```

**If the topic already exists:**
```sh
Topic already exists
```

### 6. **List All Topics**
**You can list all existing topics with a GET request:**
```sh
curl http://localhost:8080/list-topics
```

**Example response:**
```sh
{"topics":["test","chat","sports"]}
```


---

## 🏗️ Roadmap

- [x] Topic creation via API
- [x] Schema Registry
- [ ] Multi-broker clustering
- [ ] Replication and fault tolerance
- [ ] Consumer groups
- [ ] Metrics and monitoring

---

## 🤝 Contributing

Pull requests, ideas, and issue reports are always welcome!

1. **Fork the repo**
2. **Create your feature branch**
    ```sh
    git checkout -b my-feature
    ```
3. **Commit your changes**
    ```sh
    git commit -am 'Add feature'
    ```
4. **Push to the branch**
    ```sh
    git push origin my-feature
    ```
5. **Open a pull request**

---

## 📄 License

MIT License.
See [LICENSE](LICENSE).


Inspired by Apache Kafka, built for learning and fun!

<p align="center"> <img src="https://img.shields.io/badge/Built%20With-Go-00ADD8?logo=go&logoColor=white" alt="Go Badge" height="24"> </p>
