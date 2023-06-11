package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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

func (c Client) Set() {}

func main() {

	// parse args and connect to node

	// create a new client
	client := Client{}

	getHostPort() //string - "127.0.0.1", string "8765"

	//first connect to server - getting info about servers of Swarm
	swarmInfo, err := client.Get()
	if err != nil {
		log.Fatal()
	}
	client.printKnownNodes(swarmInfo)

	// goroutine that receives heartbeats and changes nodes

	// command execution loop
	for {
		isInputCorrect, stopReading, command := getCommand()
		if isInputCorrect {
			// command execute
			fmt.Println(command)
			switch client.currentCommand.Action {
			case get:
				//get
				swarm, err := client.Get()
				if err != nil {
					fmt.Println("Error in Get request")
					return
				}
				client.currentSwarm = &swarm
			case set:
				//set
			case del:
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
