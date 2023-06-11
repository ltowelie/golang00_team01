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
	Host         string             `json:"host"`
	Port         string             `json:"port"`
	Mu			 sync.Mutex			`json:"-"`
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
	// Nodes []*Node
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

func (s *Swarm) HandleHeartBeatRequest(w http.ResponseWriter, r *http.Request) {

	log.Printf("Got heartbeat signal request")

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(s)
	if err != nil {
		_, err = fmt.Fprintf(os.Stderr, "error encoding swarm: %s\n", err)
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
	if flagCount  {
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
	http.HandleFunc("/getHeartBeat", s.HandleHeartBeatRequest)
	http.HandleFunc("/getRecord", s.HandleGetRecord)
	http.HandleFunc("/setRecord", s.HandleSetRecord)
	http.HandleFunc("/delRecord", s.HandleDelRecord)

	err := http.ListenAndServe(s.ThisNode.Host+":"+s.ThisNode.Port, nil)
	if err != nil {
		log.Fatal(err)
	}

}
