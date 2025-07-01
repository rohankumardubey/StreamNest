// package main

// import (
// 	"bufio"
// 	"bytes"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"io"
// 	"net/http"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"
// )

// type PartitionInfo struct {
// 	Partition int    `json:"partition"`
// 	Broker    string `json:"broker"`
// }

// type TopicMetadata struct {
// 	Partitions []PartitionInfo `json:"partitions"`
// }

// type MetadataResponse struct {
// 	Topics map[string]TopicMetadata `json:"topic_partitions"`
// }

// type Broker struct {
// 	ID        int
// 	Address   string
// 	Peers     []string
// 	Port      int
// 	topics    map[string][][]string
// 	ownership map[string][]string
// 	mu        sync.Mutex
// }

// func NewBroker(id, port int, peers []string) *Broker {
// 	addr := fmt.Sprintf("localhost:%d", port)
// 	return &Broker{
// 		ID:        id,
// 		Address:   addr,
// 		Peers:     peers,
// 		Port:      port,
// 		topics:    make(map[string][][]string),
// 		ownership: make(map[string][]string),
// 	}
// }

// type createTopicReq struct {
// 	Topic         string   `json:"topic"`
// 	NumPartitions int      `json:"partitions"`
// 	Owners        []string `json:"owners,omitempty"`
// }

// // POST /create-topic
// func (b *Broker) createTopicHandler(w http.ResponseWriter, r *http.Request) {
// 	var req createTopicReq
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, "invalid request", 400)
// 		return
// 	}
// 	if req.Topic == "" || req.NumPartitions <= 0 {
// 		http.Error(w, "topic+positive partitions required", 400)
// 		return
// 	}
// 	// Compute owners list ONCE
// 	all := append([]string{b.Address}, b.Peers...)
// 	owners := make([]string, req.NumPartitions)
// 	for i := 0; i < req.NumPartitions; i++ {
// 		owners[i] = all[i%len(all)]
// 	}
// 	// Install locally
// 	b.createTopicWithOwners(req.Topic, owners)
// 	fmt.Printf("[Broker %d] Created topic '%s' owners=%v\n", b.ID, req.Topic, owners)

// 	// Synchronously propagate to peers
// 	prop := createTopicReq{Topic: req.Topic, Owners: owners}
// 	body := mustJSON(prop)
// 	for _, peer := range b.Peers {
// 		if peer == b.Address {
// 			continue
// 		}
// 		url := "http://" + peer + "/internal-create-topic"
// 		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
// 		if err != nil {
// 			fmt.Printf("[Broker %d] Propagate to %s failed: %v\n", b.ID, peer, err)
// 			continue
// 		}
// 		resp.Body.Close()
// 	}
// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
// }

// // POST /internal-create-topic
// func (b *Broker) internalCreateTopicHandler(w http.ResponseWriter, r *http.Request) {
// 	var req createTopicReq
// 	json.NewDecoder(r.Body).Decode(&req)
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
// 	if _, ok := b.topics[req.Topic]; ok {
// 		w.WriteHeader(200)
// 		return
// 	}
// 	b.ownership[req.Topic] = req.Owners
// 	partitions := make([][]string, len(req.Owners))
// 	for i := range partitions {
// 		partitions[i] = []string{}
// 	}
// 	b.topics[req.Topic] = partitions
// 	fmt.Printf("[Broker %d] (internal) Created topic '%s' owners=%v\n", b.ID, req.Topic, req.Owners)
// 	w.WriteHeader(200)
// }

// func (b *Broker) createTopicWithOwners(topic string, owners []string) {
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
// 	b.ownership[topic] = owners
// 	partitions := make([][]string, len(owners))
// 	for i := range partitions {
// 		partitions[i] = []string{}
// 	}
// 	b.topics[topic] = partitions
// }

// // GET /metadata
// func (b *Broker) metadataHandler(w http.ResponseWriter, r *http.Request) {
// 	b.mu.Lock(); defer b.mu.Unlock()
// 	resp := MetadataResponse{Topics: make(map[string]TopicMetadata)}
// 	for t, owners := range b.ownership {
// 		var parts []PartitionInfo
// 		for i, o := range owners {
// 			parts = append(parts, PartitionInfo{i, o})
// 		}
// 		resp.Topics[t] = TopicMetadata{parts}
// 	}
// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(resp)
// }

// // GET /list-topics
// func (b *Broker) listTopicsHandler(w http.ResponseWriter, r *http.Request) {
// 	b.mu.Lock(); defer b.mu.Unlock()
// 	var ts []string
// 	for t := range b.ownership {
// 		ts = append(ts, t)
// 	}
// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(map[string][]string{"topics": ts})
// }

// // POST /produce
// func (b *Broker) produceHandler(w http.ResponseWriter, r *http.Request) {
// 	var req struct {
// 		Topic     string `json:"topic"`
// 		Partition int    `json:"partition"`
// 		Message   string `json:"message"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, "invalid", 400); return
// 	}
// 	b.mu.Lock()
// 	owners, ok := b.ownership[req.Topic]
// 	b.mu.Unlock()
// 	if !ok || req.Partition<0 || req.Partition>=len(owners) {
// 		http.Error(w,"unknown topic/partition",404); return
// 	}
// 	owner := owners[req.Partition]
// 	if owner!=b.Address {
// 		// forward
// 		resp, err := http.Post("http://"+owner+"/produce","application/json",bytes.NewBuffer(mustJSON(req)))
// 		if err!=nil { http.Error(w,"forward fail",500); return }
// 		defer resp.Body.Close()
// 		io.Copy(w,resp.Body)
// 		return
// 	}
// 	// local
// 	b.mu.Lock()
// 	slice := &b.topics[req.Topic][req.Partition]
// 	*slice = append(*slice, req.Message)
// 	offset := len(*slice)-1
// 	b.mu.Unlock()
// 	fmt.Printf("[Broker %d] + topic=%s p=%d off=%d\n",b.ID,req.Topic,req.Partition,offset)
// 	json.NewEncoder(w).Encode(map[string]int{"offset":offset})
// }

// // GET /consume
// func (b *Broker) consumeHandler(w http.ResponseWriter, r *http.Request) {
// 	t := r.URL.Query().Get("topic")
// 	p, _ := strconv.Atoi(r.URL.Query().Get("partition"))
// 	o, _ := strconv.Atoi(r.URL.Query().Get("offset"))

// 	b.mu.Lock()
// 	owners, ok := b.ownership[t]
// 	b.mu.Unlock()
// 	if !ok || p<0 || p>=len(owners) {
// 		http.Error(w,"unknown topic/partition",404); return
// 	}
// 	owner := owners[p]
// 	if owner!=b.Address {
// 		resp, err := http.Get(fmt.Sprintf(
// 			"http://%s/consume?topic=%s&partition=%d&offset=%d",
// 			owner,t,p,o))
// 		if err!=nil { http.Error(w,"forward fail",500); return }
// 		defer resp.Body.Close()
// 		io.Copy(w,resp.Body)
// 		return
// 	}
// 	// local
// 	b.mu.Lock()
// 	msgs := b.topics[t][p]
// 	b.mu.Unlock()
// 	if o<0||o>=len(msgs) {
// 		w.WriteHeader(http.StatusNoContent)
// 		return
// 	}
// 	fmt.Printf("[Broker %d] - topic=%s p=%d off=%d\n",b.ID,t,p,o)
// 	json.NewEncoder(w).Encode(map[string]interface{}{"offset":o,"message":msgs[o]})
// }

// func mustJSON(v interface{}) []byte{
// 	b,_:=json.Marshal(v)
// 	return b
// }

// func runBroker(id,port int,peers []string){
// 	b:=NewBroker(id,port,peers)
// 	http.HandleFunc("/create-topic",b.createTopicHandler)
// 	http.HandleFunc("/internal-create-topic",b.internalCreateTopicHandler)
// 	http.HandleFunc("/metadata",b.metadataHandler)
// 	http.HandleFunc("/list-topics",b.listTopicsHandler)
// 	http.HandleFunc("/produce",b.produceHandler)
// 	http.HandleFunc("/consume",b.consumeHandler)
// 	fmt.Printf("Broker %d on :%d\n",id,port)
// 	http.ListenAndServe(fmt.Sprintf(":%d",port),nil)
// }

// func runProducer(meta string){
// 	r:=bufio.NewReader(os.Stdin)
// 	fmt.Print("Enter topic: ")
// 	t, _ := r.ReadString('\n'); t=strings.TrimSpace(t)
// 	resp,_:=http.Get("http://"+meta+"/metadata")
// 	var m MetadataResponse
// 	json.NewDecoder(resp.Body).Decode(&m)
// 	parts:=m.Topics[t].Partitions
// 	fmt.Println("Partitions:")
// 	for _,p:=range parts{fmt.Printf(" %d=>%s\n",p.Partition,p.Broker)}
// 	fmt.Print("Partition?> ")
// 	pl,_:=r.ReadString('\n'); p,_:=strconv.Atoi(strings.TrimSpace(pl))
// 	fmt.Println("Type msg or 'exit'")
// 	for {
// 		fmt.Print("> ")
// 		txt,_:=r.ReadString('\n'); txt=strings.TrimSpace(txt)
// 		if txt=="exit"{break}
// 		req:=map[string]interface{}{"topic":t,"partition":p,"message":txt}
// 		resp,_:=http.Post("http://"+meta+"/produce","application/json",bytes.NewBuffer(mustJSON(req)))
// 		var out map[string]int
// 		json.NewDecoder(resp.Body).Decode(&out)
// 		fmt.Println("offset:",out["offset"])
// 		resp.Body.Close()
// 	}
// }

// func runConsumer(meta string){
// 	r:=bufio.NewReader(os.Stdin)
// 	fmt.Print("Enter topic: ")
// 	t,_:=r.ReadString('\n'); t=strings.TrimSpace(t)
// 	resp,_:=http.Get("http://"+meta+"/metadata")
// 	var m MetadataResponse
// 	json.NewDecoder(resp.Body).Decode(&m)
// 	parts:=m.Topics[t].Partitions
// 	fmt.Println("Partitions:")
// 	for _,p:=range parts{fmt.Printf(" %d=>%s\n",p.Partition,p.Broker)}
// 	fmt.Print("Partition?> ")
// 	pl,_:=r.ReadString('\n'); p,_:=strconv.Atoi(strings.TrimSpace(pl))

// 	off:=0
// 	for {
// 		url:=fmt.Sprintf("http://%s/consume?topic=%s&partition=%d&offset=%d",meta,t,p,off)
// 		resp,err:=http.Get(url)
// 		if err!=nil{time.Sleep(time.Second);continue}
// 		if resp.StatusCode==204{resp.Body.Close();time.Sleep(500*time.Millisecond);continue}
// 		if resp.StatusCode!=200{resp.Body.Close();time.Sleep(time.Second);continue}
// 		var d struct{Offset int;Message string}
// 		json.NewDecoder(resp.Body).Decode(&d)
// 		resp.Body.Close()
// 		fmt.Printf("[Offset %d] %s\n",d.Offset,d.Message)
// 		off++
// 	}
// }

// func main(){
// 	if len(os.Args)<2{
// 		fmt.Println("Usage:\n broker --id=1 --port=8080 --peers=a,b\n producer --meta=host:port\n consumer --meta=host:port")
// 		return
// 	}
// 	switch os.Args[1]{
// 	case "broker":
// 		fs:=flag.NewFlagSet("broker",flag.ExitOnError)
// 		id:=fs.Int("id",1,"broker id")
// 		port:=fs.Int("port",8080,"port")
// 		peers:=fs.String("peers","","comma sep peers")
// 		fs.Parse(os.Args[2:])
// 		pl:=[]string{}
// 		if *peers!=""{pl=strings.Split(*peers,",")}
// 		runBroker(*id,*port,pl)
// 	case "producer":
// 		fs:=flag.NewFlagSet("producer",flag.ExitOnError)
// 		meta:=fs.String("meta","localhost:8080","meta")
// 		fs.Parse(os.Args[2:])
// 		runProducer(*meta)
// 	case "consumer":
// 		fs:=flag.NewFlagSet("consumer",flag.ExitOnError)
// 		meta:=fs.String("meta","localhost:8080","meta")
// 		fs.Parse(os.Args[2:])
// 		runConsumer(*meta)
// 	default:
// 		fmt.Println("Unknown mode")
// 	}
// }
