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

type Command struct {
	Action string
	Args   []string
}

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

func (s *Swarm) SendHeartbeatToAllNodes() {

	wg := new(sync.WaitGroup)
	s.Mu.Lock()
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
	s.Mu.Unlock()
	wg.Wait()
}

func (n *Node) SendHeartBeat(addr string) *Swarm {

	client := &http.Client{}

	jsonBody, err := json.Marshal(n)
	if err != nil {
		log.Fatal(err)
	}
	bodyReader := bytes.NewReader(jsonBody)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/heartBeat", bodyReader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error create heartbeat to %s: %s\n", addr, err)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error send heartbeat to %s: %s\n", addr, err)
		return nil
	}

	var swarm Swarm

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading getServer request body from %s: %s\n", addr, err)
	}
	err = json.Unmarshal(body, &swarm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error unmarshalling swarm struct in getServer request from %s: %s\n", addr, err)
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(res.Body)
	return &swarm

}

func (s *Swarm) HandleHeartBeat(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(s)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error encoding swarm in heartbeat: %s\n", err)
	}

	switch r.URL.Path {
	case "/heartBeat":
		var node *Node

		body, err := io.ReadAll(r.Body)
		if err != nil {
			_, err = fmt.Fprintf(os.Stderr, "error reading request body: %s\n", err)
			if err != nil {
				log.Printf("%s\n", err)
			}
		}
		err = json.Unmarshal(body, &node)

		log.Printf("Got heartbeat signal from %s\n", node.Addr)

		if err != nil {
			_, err = fmt.Fprintf(os.Stderr, "error unmarshalling heartbeat signal node: %s\n", err)
			if err != nil {
				log.Printf("%s\n", err)
			}
		}
		s.Mu.Lock()
		s.Nodes[node.Addr] = node
		s.Nodes[node.Addr].HeartBeat = time.Now()
		s.Mu.Unlock()
	}
}

func successFind(w http.ResponseWriter, val *Record) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")

	reqBody, err := json.Marshal(val)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error marshalling record: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}
	w.Write(reqBody)
}

func noRecordFind(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
}

func (s *Swarm) HandleGetRecord(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got GET_RECORD")

	var findRecord Command
	body, err := io.ReadAll(r.Body)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error reading GET_request body: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}

	err = json.Unmarshal(body, &findRecord)
	val, ok := s.ThisNode.DB[findRecord.Args[0]]

	if ok {
		successFind(w, val)
	} else {
		noRecordFind(w)
	}
}

func (s *Swarm) HandleSetRecord(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got SET_RECORD")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error reading GET_request body: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}

	var findRecord Command
	err = json.Unmarshal(body, &findRecord)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error unmarshalling SET request: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}
	uuid := findRecord.Args[0]
	value := findRecord.Args[1]
	_, ok := s.ThisNode.DB[uuid]
	var flagCount bool
	if !ok {
		flagCount = true
	}

	var tempRecord Record
	tempRecord.Value = value
	s.ThisNode.Mu.Lock()
	s.ThisNode.DB[uuid] = &tempRecord
	if flagCount {
		s.ThisNode.RecordsCount++
	}
	s.ThisNode.Mu.Unlock()

	w.WriteHeader(http.StatusOK)

	// if flagCount{
	// 	w.WriteHeader(http.StatusCreated)
	// } else {
	// 	w.WriteHeader(http.StatusAccepted)
	// }
}

func (s *Swarm) HandleDelRecord(w http.ResponseWriter, r *http.Request) {
	//	var elem *Swarm
	log.Printf("Got DEL_RECORD")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error reading GET_request body: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}

	var findRecord Command
	err = json.Unmarshal(body, &findRecord)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error unmarshalling SET request: %s\n", err)
		if err != nil {
			log.Printf("%s\n", err)
		}
	}

	uuid := findRecord.Args[0]
	_, ok := s.ThisNode.DB[uuid]

	if ok {
		s.ThisNode.Mu.Lock()
		delete(s.ThisNode.DB, uuid)
		s.ThisNode.RecordsCount--
		s.ThisNode.Mu.Unlock()
	}

	w.WriteHeader(http.StatusOK)

	// if flagCount{
	// 	w.WriteHeader(http.StatusCreated)
	// } else {
	// 	w.WriteHeader(http.StatusAccepted)
	// }
}

func (s *Swarm) ServeRequests() {

	http.HandleFunc("/heartBeat", s.HandleHeartBeat)
	http.HandleFunc("/getServer", s.HandleHeartBeat)
	http.HandleFunc("/getRecord", s.HandleGetRecord)
	http.HandleFunc("/setRecord", s.HandleSetRecord)
	http.HandleFunc("/delRecord", s.HandleDelRecord)

	err := http.ListenAndServe(s.ThisNode.Addr, nil)
	if err != nil {
		log.Fatal(err)
	}
}
