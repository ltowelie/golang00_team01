package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"team01/node"
	"time"

	"github.com/google/uuid"
)

const (
	get = "GET"
	set = "SET"
	del = "DELETE"
)

type Command struct {
	Action string   `json:"command"`
	Args   []string `json:"args"`
}

type Client struct {
	host           string
	port           string
	currentSwarm   *node.Swarm
	currentCommand Command
	Mu             sync.Mutex
}

type NodeStruct struct {
	key   string
	value *node.Node
}

func getCommand() (isInputCorrect, stopReading bool, command *Command) {

	command = nil
	stopReading = false
	isInputCorrect = true
	userInput := bufio.NewReader(os.Stdin)
	userVal, err := userInput.ReadString('\n')

	if err != nil && err != io.EOF {
		isInputCorrect = false
		_, err := fmt.Fprint(os.Stderr, "Input error.\n")
		if err != nil {
			return
		}
	} else if io.EOF == err {
		stopReading = true
	}

	input := strings.TrimSpace(userVal)
	if isInputCorrect && input == "" {
		if err != io.EOF {
			_, err := fmt.Fprint(os.Stderr, "Empty value.\n")
			if err != nil {
				return
			}
		}
		isInputCorrect = false
	} else if isInputCorrect {
		stringsFields := strings.SplitN(input, " ", 3)
		if len(stringsFields) < 2 {
			_, err = fmt.Fprint(os.Stderr, "Wrong command value.\n")
			if err != nil {
				return
			}
		} else {
			if stringsFields[0] == get || stringsFields[0] == set || stringsFields[0] == del {
				args := stringsFields[1:]
				if (stringsFields[0] == get || stringsFields[0] == del) && len(args) != 1 {
					_, err = fmt.Fprint(os.Stderr, "Wrong command arguments count1.\n")
					if err != nil {
						return
					}
				} else if stringsFields[0] == set && len(args) != 2 {
					_, err = fmt.Fprint(os.Stderr, "Wrong command arguments count2.\n")
					if err != nil {
						return
					}
				} else if !isUUID4(args[0]) {
					_, err = fmt.Fprint(os.Stderr, "Error: Key is not a proper UUID4\n")
					return
				} else {
					command = &Command{
						Action: stringsFields[0],
						Args:   args,
					}
				}
			} else {
				_, err = fmt.Fprint(os.Stderr, "Unsupported command.\n")
				if err != nil {
					return
				}
			}
		}
	}
	return
}

func (c Client) printKnownNodes() {
	fmt.Printf("Connected to a database of Warehouse 13 at %s:%s\n", c.host, c.port)
	fmt.Println("Known nodes:")
	c.Mu.Lock()
	nodes := c.currentSwarm.Nodes
	c.Mu.Unlock()
	for _, val := range nodes {
		fmt.Printf("%v\n", (*val).Addr)
	}
}

func (c Client) getServer() (swarm node.Swarm, err error) {
	client := &http.Client{}

	resp, err := client.Get("http://" + c.host + ":" + c.port + "/getServer")
	if err != nil {
		return swarm, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return swarm, err
	}
	json.Unmarshal(body, &swarm)
	return swarm, nil
}

func (c Client) setRecord() error {
	c.Mu.Lock()
	nodes := c.currentSwarm.Nodes
	c.Mu.Unlock()
	client := &http.Client{}
	var setNodes []string
	for serverAddr, server := range nodes {
		getStr := "{\"command\": \"GET\", \"args\": [\"" + c.currentCommand.Args[0] + "\"]}"
		req, err := http.NewRequest(get, "http://"+(*server).Addr+"/getRecord", bytes.NewBufferString(getStr))
		if err != nil {
			log.Println("Error: ", err)
			return err
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if resp.StatusCode == 200 {
			setNodes = append(setNodes, serverAddr)
		}
	}
	if len(setNodes) == 0 {
		// sorting map of nodes
		var nodesStruct []NodeStruct
		for key, val := range nodes {
			nS := &NodeStruct{
				key:   key,
				value: val,
			}
			nodesStruct = append(nodesStruct, *nS)
		}
		sort.Slice(nodesStruct, func(i, j int) bool {
			return nodesStruct[i].value.RecordsCount < nodesStruct[j].value.RecordsCount
		})

		if len(nodesStruct) >= 2 {
			for i, val := range nodesStruct {
				if i < 2 {
					setNodes = append(setNodes, val.key)
					fmt.Println("val.value.RecordsCount: ", val.value.RecordsCount)
				}
			}
		}
	}
	// send set request to server
	for _, server := range setNodes {
		body, err := json.Marshal(c.currentCommand)
		if err != nil {
			log.Println(err)
			return err
		}
		bodyReader := bytes.NewReader(body)
		req, err := http.NewRequest(set, "http://"+server+"/setRecord", bodyReader)
		if err != nil {
			log.Println("Error: ", err)
			return err
		}
		req.Header.Add("Content-Type", "application/json")
		_, err = client.Do(req)
		if err != nil {
			fmt.Println("Request wasn't send: ", err)
			return err
		}
	}
	if len(setNodes) != 0 {
		fmt.Printf("Created (%d replicas)\n", len(setNodes))
	} else {
		fmt.Printf("Failed to write/read an entry\n")
	}
	return nil
}

func (c *Client) getHeartBeat() error {
	swarmInfo, err := c.getServer()
	if err != nil {
		addr := c.host + ":" + c.port
		c.Mu.Lock()
		delete(c.currentSwarm.Nodes, addr)
		c.Mu.Unlock()
		for key, _ := range c.currentSwarm.Nodes {
			arr := strings.Split(key, ":")
			if c.host == arr[0] && c.port != arr[1] {
				c.Mu.Lock()
				c.host = arr[0]
				c.port = arr[1]
				c.Mu.Unlock()
				fmt.Printf("Reconnected to a database of Warehouse 13 at %s:%s\n", c.host, c.port)
				c.printKnownNodes()
				if len(c.currentSwarm.Nodes) < 2 {
					fmt.Println("WARNING: cluster size (1) is smaller than a replication factor (2)!")
				}
				break
			}
		}

	}
	c.Mu.Lock()
	c.currentSwarm = &swarmInfo
	c.Mu.Unlock()
	return nil
}

func (c Client) getRecord() (statusCode int, value node.Record) {
	c.Mu.Lock()
	nodes := c.currentSwarm.Nodes
	c.Mu.Unlock()
	for _, server := range nodes {
		client := &http.Client{}
		body, err := json.Marshal(c.currentCommand)
		// fmt.Println("body GET: ", body)
		if err != nil {
			log.Println(err)
			return
		}
		bodyReader := bytes.NewReader(body)
		req, err := http.NewRequest(get, "http://"+server.Addr+"/getRecord", bodyReader)
		if err != nil {
			log.Println("Error: ", err)
			return
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("Request wasn't send: ", err)
			return
		}
		if resp.StatusCode == 200 {
			statusCode = resp.StatusCode
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Println("Error: ", err)
				return
			}
			json.Unmarshal(body, &value)
			break
		}
	}
	if statusCode == 0 {
		value.Value = "Not found"
		statusCode = 404
	}
	return statusCode, value
}

func newClient() Client {
	var host, port string

	hostUsageStr := "host to listen (must be ip or hostname)"
	portUsageStr := "port to listen (must be 1-65535)"

	flag.StringVar(&host, "H", "", hostUsageStr)
	flag.StringVar(&port, "P", "", portUsageStr)
	flag.Parse()
	stop := port == ""
	if stop == false {
		iPort, err := strconv.Atoi(port)
		if err != nil {
			stop = true
		} else if iPort < 1 || iPort > 65535 {
			stop = true
		}
		stop = false
	}

	if stop {
		fmt.Printf("Usage %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	client := Client{
		host: host,
		port: port,
	}

	return client
}

func isUUID4(str string) bool {
	key, err := uuid.ParseBytes([]byte(str))
	if err != nil {
		return false
	}
	v := key.Version()
	if v != uuid.Version(byte(4)) {
		return false
	}
	return true
}

func main() {
	// create a new client
	client := newClient()

	//first connect to node - getting info about servers of Swarm
	swarmInfo, err := client.getServer()
	if err != nil {
		log.Fatalln("Error on server")
	}
	client.currentSwarm = &swarmInfo
	client.printKnownNodes()

	wg := new(sync.WaitGroup)
	wg.Add(1)
	// goroutine that receives heartbeats and changes nodes
	go func(wg *sync.WaitGroup, client *Client) {
		ticker := time.Tick(time.Second)
		for {
			// log.Println("Отправка heartbeat сигнала нодам")
			<-ticker
			err := client.getHeartBeat()
			if err != nil {
				break
			}
		}
		wg.Done()
	}(wg, &client)

	// command execution loop
	for {
		isInputCorrect, stopReading, command := getCommand()
		if isInputCorrect && command != nil {
			// command execute
			client.currentCommand = *command
			switch client.currentCommand.Action {
			case get:
				// log.Println("Get request")
				//get
				statusCode, value := client.getRecord()
				switch statusCode {
				case 200:
					fmt.Println(value)
				case 404:
					fmt.Println(value)
					continue
				}
			case set:
				// log.Println("Set request")
				//set
				err := client.setRecord()
				if err != nil {
					fmt.Println("Error in Set request: ", err)
					continue
				}
			case del:
				// log.Println("Delete request")
				//delete
			default:
				fmt.Println("Unknown command, you can use only GET, SET, DELETE methods")
				continue
			}
		}
		if stopReading {
			fmt.Printf("\n")
			break
		}
	}
	wg.Wait()
}
