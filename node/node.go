package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Node struct {
	Addr         string             `json:"address"`
	RecordsCount int                `json:"records_count"`
	Keys         []string           `json:"-"`
	Mu           sync.Mutex         `json:"-"`
	DB           map[string]*Record `json:"-"`
	HeartBeat    time.Time          `json:"-"`
}

type Swarm struct {
	ThisNode          *Node            `json:"node"`
	Mu                sync.Mutex       `json:"-"`
	Nodes             map[string]*Node `json:"nodes"`
	ReplicationFactor int              `json:"replication_factor"`
}

type Record struct {
	Value string `json:"value"`
}

func (s *Swarm) SendHeartbeatToAllNodes() error {

	wg := new(sync.WaitGroup)

	for _, node := range s.Nodes {

		if s.ThisNode == node {
			continue
		}
		wg.Add(1)
		go func(s *Swarm, node *Node, wg *sync.WaitGroup) {
			defer wg.Done()
			s.ThisNode.SendHeartBeat(node.Addr)
		}(s, node, wg)
	}
	wg.Wait()
	return nil
}

func (n Node) SendHeartBeat(addr string) {

	client := &http.Client{}

	jsonBody, err := json.Marshal(n)
	if err != nil {
		log.Fatal(err)
	}
	bodyReader := bytes.NewReader(jsonBody)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/heartBeat", bodyReader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error create request: %s\n", err)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error send request: %s\n", err)
	}

	if res != nil {
		defer func(Body io.ReadCloser) {
			err = Body.Close()
			if err != nil {

			}
		}(res.Body)
	}

}

func (n Node) SendGetServer(addr string) *Swarm {

	client := &http.Client{}

	jsonBody, err := json.Marshal(n)
	if err != nil {
		log.Fatal(err)
	}
	bodyReader := bytes.NewReader(jsonBody)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/getServer", bodyReader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error create request: %s\n", err)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error send request: %s\n", err)
	}

	var swarm Swarm

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading request body: %s\n", err)
	}
	err = json.Unmarshal(body, &swarm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error unmarshalling swarm: %s\n", err)
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(res.Body)
	return &swarm
}

func (s *Swarm) HandleGetServer(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(s)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error encoding swarm in get server: %s\n", err)
	}

	var elem *Node

	body, err := io.ReadAll(r.Body)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error reading request body: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}
	err = json.Unmarshal(body, &elem)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error unmarshalling heartbeat signal node: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}

	log.Printf("Got get server signal from %s\n", elem.Addr)

	s.Mu.Lock()
	s.Nodes[elem.Addr] = elem
	s.Nodes[elem.Addr].HeartBeat = time.Now()
	s.Mu.Unlock()

}

func (s *Swarm) HandleHeartBeat(w http.ResponseWriter, r *http.Request) {
	var elem *Node

	_ = w

	body, err := io.ReadAll(r.Body)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error reading request body: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}
	err = json.Unmarshal(body, &elem)

	log.Printf("Got heartbeat signal from %s\n", elem.Addr)

	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error unmarshalling heartbeat signal node: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}
	s.Mu.Lock()
	s.Nodes[elem.Addr] = elem
	s.Nodes[elem.Addr].HeartBeat = time.Now()
	s.Mu.Unlock()
}

func (s *Swarm) ServeRequests() {

	http.HandleFunc("/heartBeat", s.HandleHeartBeat)
	http.HandleFunc("/getServer", s.HandleGetServer)
	err := http.ListenAndServe(s.ThisNode.Addr, nil)
	if err != nil {
		log.Fatal(err)
	}

}
