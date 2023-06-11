package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"team01/node"
)

const (
	get = "GET"
	set = "SET"
	del = "DELETE"
)

type Command struct {
	Action string
	Args   []string
}

type Client struct {
	host           string
	port           string
	currentSwarm   *node.Swarm
	currentCommand Command
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

func getUrl() (urlS string) {

	return urlS
}

func (c Client) printKnownNodes(swarm node.Swarm) {
	fmt.Println("Connected to a database of Warehouse 13 at ", c.host, ":", c.port)
	fmt.Println("Known nodes:")
	for _, val := range swarm.Nodes {
		fmt.Println(val.Host, ":", val.Port)
	}
}

func (c Client) Get() (swarm node.Swarm, err error) {
	client := &http.Client{}

	resp, err := client.Get("https://" + c.host + ":" + c.port + "/getHeartBeat")
	if err != nil {
		fmt.Println("Error from Get method: ", err)
		return swarm, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error from Get method: ", err)
		return swarm, err
	}
	json.Unmarshal(body, &swarm)
	return swarm, nil
}

func (c Client) Set() error {
	client := &http.Client{}

	body := ""
	for _, arg := range c.currentCommand.Args {
		body = body + arg + "&"
	}
	fmt.Println("body: ", body[:len(body)-1])
	req, err := http.NewRequest(set, "http://"+c.host+":"+c.port+"/setRequest", bytes.NewBufferString(body[:len(body)-1]))
	if err != nil {
		fmt.Println("Error in Set request: ", err)
		return err
	}
	req.Header.Add("Accept", "text/html")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error in Set request: ", err)
		return err
	}
	defer resp.Body.Close()
	return nil
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

	// getHostPort() //string - "127.0.0.1", string "8765"

	//first connect to server - getting info about servers of Swarm
	//swarmInfo, err := client.Get()
	//if err != nil {
	//	log.Fatalln("Error on server")
	//}
	//client.printKnownNodes(swarmInfo)

	// goroutine that receives heartbeats and changes nodes

	// command execution loop
	for {
		isInputCorrect, stopReading, command := getCommand()
		fmt.Println("main | command: ", command.Action)
		if isInputCorrect {
			// command execute
			client.currentCommand = *command

			switch client.currentCommand.Action {
			case get:
				log.Println("Get request")
				//get
				swarm, err := client.Get()
				if err != nil {
					fmt.Println("Error in Get request: ", err)
					return
				}
				client.currentSwarm = &swarm
			case set:
				log.Println("Set request")
				//set
				err := client.Set()
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

}
