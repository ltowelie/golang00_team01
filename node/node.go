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
	Host         string             `json:"host"`
	Port         string             `json:"port"`
	RecordsCount int                `json:"records_count"`
	Keys         []string           `json:"-"`
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
	Nodes []*Node
	Value string
}

func (s *Swarm) SendHeartbeatToAllNodes() error {

	wg := new(sync.WaitGroup)

	for _, node := range s.Nodes {

		if s.ThisNode == node {
			continue
		}
		wg.Add(1)
		go func(node *Node, wg *sync.WaitGroup) {
			defer wg.Done()

			client := &http.Client{}

			jsonBody, err := json.Marshal(s.ThisNode)
			if err != nil {
				log.Fatal(err)
			}
			bodyReader := bytes.NewReader(jsonBody)

			req, err := http.NewRequest(http.MethodPost, "http://"+node.Host+":"+node.Port+"/HeartBeat", bodyReader)
			if err != nil {
				log.Fatal(err)
			}
			req.Header.Add("Content-Type", "application/json")

			res, err := client.Do(req)
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Fatal(err)
			}
		}(node, wg)
	}
	wg.Wait()
	return nil
}

func (n Node) SendHeartBeat(host, port string) (*Swarm, error) {
	var swarm Swarm

	client := &http.Client{}

	jsonBody, err := json.Marshal(n)
	if err != nil {
		log.Fatal(err)
	}
	bodyReader := bytes.NewReader(jsonBody)

	req, err := http.NewRequest(http.MethodPost, "http://"+host+":"+port+"/heartBeat", bodyReader)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

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

	return &swarm, nil
}

func (n Node) HandleHeartBeat(w http.ResponseWriter, r *http.Request) {
	var elem *Swarm
	log.Printf("Got heartbeat signal")

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

}

func (s *Swarm) HandleHeartBeat(w http.ResponseWriter, r *http.Request) {
	var elem *Swarm
	log.Printf("Got heartbeat signal")

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

	s.Mu.Lock()
	key := elem.ThisNode.Host + ":" + elem.ThisNode.Port
	s.Nodes[key] = elem.ThisNode
	s.Nodes[key].HeartBeat = time.Now()
	s.Mu.Unlock()
}

func (s *Swarm) ServeRequests() {

	http.HandleFunc("/heartBeat", s.HandleHeartBeat)
	http.HandleFunc("/getHeartBeat", s.ThisNode.HandleHeartBeat)
	err := http.ListenAndServe(s.ThisNode.Host+":"+s.ThisNode.Port, nil)
	if err != nil {
		log.Fatal(err)
	}

}
