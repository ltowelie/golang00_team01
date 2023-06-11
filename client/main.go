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
		stringsFields := strings.Fields(input)
		if len(stringsFields) < 2 {
			_, err = fmt.Fprint(os.Stderr, "Wrong command value.\n")
			if err != nil {
				return
			}
		} else {
			if stringsFields[0] == get || stringsFields[0] == set || stringsFields[0] == del {
				args := stringsFields[1:]

				if (stringsFields[0] == get || stringsFields[0] == del) && len(args) != 1 {
					_, err = fmt.Fprint(os.Stderr, "Wrong command arguments count.\n")
					if err != nil {
						return
					}
				} else if stringsFields[0] == set && len(args) != 2 {
					_, err = fmt.Fprint(os.Stderr, "Wrong command arguments count.\n")
					if err != nil {
						return
					}
				} else if !isUUID4(args[0]) {
					_, err = fmt.Fprint(os.Stderr, "Error: Key is not a proper UUID4\n")
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

	resp, err := client.Get("http://" + c.host + ":" + c.port + "/getHeartBeat")
	if err != nil {
		fmt.Println("Error from getServer: ", err)
		return swarm, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error from getServer: ", err)
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

	for _, node := range nodes {
		getStr := "{\"command\": \"GET\", \"args\": [\"" + c.currentCommand.Args[0] + "\"]}"
		// fmt.Println("getStr: ", getStr)
		// statusCode, _ := c.getRecord()
		req, err := http.NewRequest(get, "http://"+(*node).Addr+"/findRecord", bytes.NewBufferString(getStr))
		if err != nil {
			log.Println("Error: ", err)
			return
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if resp.StatusCode == 404 {
			continue
		}

		body, err := json.Marshal(c.currentCommand)
		req, err := http.NewRequest(set, "http://"+val.Addr+"/setRecord", bytes.NewBuffer(body))
		if err != nil {
			fmt.Println("Error in Set request: ", err)
			return err
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("Request wasn't send: ", err)
			return err
		}
		defer resp.Body.Close()
	}
	return nil
}

func (c Client) getHeartBeat() error {
	swarmInfo, err := c.getServer()
	if err != nil {
		log.Println("Error on server: ", err)
		return err
	}
	c.currentSwarm = &swarmInfo
	return nil
}

func (c Client) getRecord() (statusCode int, value node.Record) {
	c.Mu.Lock()
	nodes := c.currentSwarm.Nodes
	c.Mu.Unlock()
	for _, server := range nodes {
		client := &http.Client{}
		body, err := json.Marshal(c.currentCommand)
		req, err := http.NewRequest(get, "http://"+server.Addr+"/findRecord", bytes.NewBuffer(body))
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
		fmt.Println("value json: ", value)
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
	go func(wg *sync.WaitGroup) {
		ticker := time.Tick(time.Second)
		for {
			//log.Println("Отправка heartbeat сигнала нодам")
			<-ticker
			err := client.getHeartBeat()
			if err != nil {
				break
			}
		}
		wg.Done()
	}(wg)

	// command execution loop
	for {
		isInputCorrect, stopReading, command := getCommand()
		// fmt.Println("main | command: ", command.Action)
		if isInputCorrect {
			// command execute
			client.currentCommand = *command

			switch client.currentCommand.Action {
			case get:
				log.Println("Get request")
				//get
				statusCode, value := client.getRecord()
				switch statusCode {
				case 200:
					fmt.Println(value)
				case 404:
					fmt.Println("Record not found")
					return
				}
			case set:
				log.Println("Set request")
				//set
				err := client.setRecord()
				if err != nil {
					fmt.Println("Error in Set request: ", err)
					return
				}
			case del:
				log.Println("Delete request")
				//delete
			default:
				fmt.Println("Unknown command, you can use only GET, SET, DELETE methods")
				break
			}
		}
		if stopReading {
			fmt.Printf("\n")
			break
		}
	}
	wg.Wait()
}
