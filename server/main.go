package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"team01/node"
	"time"
)

const (
	timeoutHB = 2
)

func nodeFromArgs() (*node.Node, string, int) {
	var host, port, existNodeHost, existNodePort string
	var replicationFactor int

	hostUsageStr := "host to listen (must be ip or hostname)"
	portUsageStr := "port to listen (must be 1-65535)"
	optional := " (optional - if you want to connect to existing swarm, but using EH+EP flags is required)"
	replicationFactorUsageStr := "replication factor"

	flag.StringVar(&host, "H", "", hostUsageStr)
	flag.StringVar(&port, "P", "", portUsageStr)
	flag.IntVar(&replicationFactor, "replication_factor", 2, replicationFactorUsageStr)
	flag.StringVar(&existNodeHost, "EH", "", hostUsageStr+optional)
	flag.StringVar(&existNodePort, "EP", "", portUsageStr+optional)
	flag.Parse()

	stop := port == "" || replicationFactor == 0
	if stop == false {
		stop = !PortIsCorrect(port)
		// Exist node host is specified, but port is not specified
		if !stop && existNodeHost == "" && existNodePort != "" {
			stop = true
		}
		// Exist node host is not specified, but port is specified
		if !stop && existNodeHost != "" && existNodePort == "" {
			stop = true
		}
		if !stop && existNodePort != "" {
			stop = !PortIsCorrect(existNodePort)
		}
	}

	if stop {
		fmt.Printf("Usage %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	nodeEx := node.Node{
		Addr:         fmt.Sprintf("%s:%s", host, port),
		RecordsCount: 0,
		DB:           make(map[string]*node.Record),
	}

	ExistAddr := fmt.Sprintf("%s:%s", existNodeHost, existNodePort)
	return &nodeEx, ExistAddr, replicationFactor
}

func PortIsCorrect(port string) bool {
	iPort, err := strconv.Atoi(port)
	if err != nil {
		return false
	} else if iPort < 1 || iPort > 65535 {
		return false
	}
	return true
}

func main() {

	thisNode, existAddr, replicationFactor := nodeFromArgs()
	log.Printf("Запуск ноды на %s\n", thisNode.Addr)

	var swarm *node.Swarm
	if existAddr != ":" {
		log.Printf("Получаем информацию о нодах из ноды %s\n", existAddr)
		var err error
		swarm = thisNode.SendGetServer(existAddr)
		swarm.ThisNode = thisNode
		swarm.Nodes[thisNode.Addr] = thisNode
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Printf("Создаем новый рой нод, первая нода (текущая) %s\n", thisNode.Addr)
		swarm = &node.Swarm{
			ThisNode:          thisNode,
			Nodes:             make(map[string]*node.Node),
			ReplicationFactor: replicationFactor,
		}
		swarm.Nodes[thisNode.Addr] = thisNode
	}

	wg := new(sync.WaitGroup)

	// Обработка входящих запросов
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		swarm.ServeRequests()
		wg.Done()
	}(wg)

	//Каждую секунду посылаем сигналы heartbeat
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		ticker := time.Tick(time.Second)
		for {
			//log.Printf("Отправка heartbeat сигнала нодам (%d)\n", len(swarm.Nodes))
			<-ticker
			err := swarm.SendHeartbeatToAllNodes()
			if err != nil {
				break
			}
		}
		wg.Done()
	}(wg)

	// Каждые N секунд проверяем heartbeat time от нод
	wg.Add(1)
	go func() {
		ticker := time.Tick(time.Second * timeoutHB)
		for {
			log.Printf("Проверка heartbeat сигналов (%d)\n", len(swarm.Nodes))
			<-ticker
			if len(swarm.Nodes) == 0 {
				break
			}
			for k, v := range swarm.Nodes {
				if k == thisNode.Addr {
					continue
				}
				if time.Now().UnixNano()-v.HeartBeat.UnixNano() > int64(time.Second*timeoutHB) {
					log.Printf("Удаление ноды %s, так как вовремя не пришел heartbeat сигнал от неё\n", v.Addr)
					swarm.Mu.Lock()
					delete(swarm.Nodes, k)
					swarm.Mu.Unlock()
				}
			}
		}
		wg.Done()
	}()
	wg.Wait()
}
