package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
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

func main() {

	// parse args and connect to node

	// goroutine that receives heartbeats and changes nodes

	// command execution loop
	for {
		isInputCorrect, stopReading, command := getCommand()
		if isInputCorrect {
			// command execute
			fmt.Println(command)
		}
		if stopReading {
			fmt.Printf("\n")
			break
		}
	}

}
