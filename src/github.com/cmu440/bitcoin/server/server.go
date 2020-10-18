package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/lsp"
)

//todo check prints

type server struct {
	lspServer       lsp.Server
	requestMap      map[int]*requestInfo // a map to link client id and corresponding request
	requestQueue    *list.List           // a list work as a queue for all requests.
	activeMiners    map[int]struct{}     // all connected miners. Use map as a set here
	availableMiners *list.List           // all available miners
	ongoingTasks    map[int]*taskUnit    // task units which are executing by the miners
}

type requestInfo struct {
	clientId        int        // id of the client
	data            string     // the request string data
	lower           uint64     // the lower bound of the range
	upper           uint64     // the upper bound of the range
	currentMinHash  uint64     // to store the min hash value
	currentMinNonce uint64     // to store the nonce for the current hash value
	taskUnitsLeft   uint64     // number of undone task units
	taskUnitsQueue  *list.List // a list of task units to be done
}

type taskUnit struct {
	clientId int    // the id of the client
	start    uint64 // the start of the calculation
	end      uint64 // the end of the calculation
}

const chunkSize = 10000

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	srv := &server{
		lspServer,
		make(map[int]*requestInfo),
		list.New(),
		make(map[int]struct{}),
		list.New(),
		make(map[int]*taskUnit),
	}
	return srv, nil
}

func (srv *server) handleMessage(connId int, payload []byte) {
	var message bitcoin.Message
	err := json.Unmarshal(payload, &message)
	if err != nil {
		return
	}
	switch message.Type {
	case bitcoin.Request:
		taskUnitsLeft := (message.Upper - message.Lower + 1) / chunkSize
		if (message.Upper-message.Lower+1)%chunkSize != 0 {
			taskUnitsLeft++
		}
		taskUnitsQueue := list.New()
		for i := uint64(0); i < taskUnitsLeft; i++ {
			unit := &taskUnit{
				connId,
				message.Lower + chunkSize*i,
				message.Lower + chunkSize*(i+1),
			}
			if unit.end > message.Upper {
				unit.end = message.Upper
			}
			taskUnitsQueue.PushBack(unit)
		}
		rInfo := &requestInfo{
			connId,
			message.Data,
			message.Lower,
			message.Upper,
			^uint64(0),
			0,
			taskUnitsLeft,
			taskUnitsQueue,
		}
		srv.requestQueue.PushBack(rInfo)
		srv.requestMap[connId] = rInfo
	case bitcoin.Join:
		srv.activeMiners[connId] = struct{}{}
		srv.availableMiners.PushBack(connId)
	case bitcoin.Result:
		clientId := srv.ongoingTasks[connId].clientId
		rInfo, exist := srv.requestMap[clientId]

		//release the miner
		delete(srv.ongoingTasks, connId)
		srv.availableMiners.PushBack(connId)

		if !exist { //the client is lost and we ignore the result here
			return
		}

		//update result from miner
		rInfo.taskUnitsLeft--
		if message.Hash < rInfo.currentMinHash {
			rInfo.currentMinHash = message.Hash
			rInfo.currentMinNonce = message.Nonce
		}

		if rInfo.taskUnitsLeft == 0 { // write back the result to the client
			resultMessage := bitcoin.NewResult(message.Hash, message.Nonce)
			payload, err := json.Marshal(resultMessage)
			if err != nil {
				return
			}
			err = srv.lspServer.Write(clientId, payload)
			if err != nil {
				return
			}
		}
	}
}

func (srv *server) handleLostConn(connId int) {
	if _, exist := srv.requestMap[connId]; exist {
		// a client is lost
		delete(srv.requestMap, connId)
	} else { //a miner is lost
		delete(srv.activeMiners, connId)
		if _, exist := srv.ongoingTasks[connId]; exist {
			//push the lost job back to the queue
			unit := srv.ongoingTasks[connId]
			srv.requestMap[unit.clientId].taskUnitsQueue.PushBack(unit)
			delete(srv.ongoingTasks, connId)
		} else {
			for e := srv.availableMiners.Front(); e != nil; e = e.Next() {
				if e.Value == connId {
					srv.availableMiners.Remove(e)
					break
				}
			}
		}
	}
}

func (srv *server) distributeTasks() {
	
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	for {
		connId, payload, err := srv.lspServer.Read()
		if err != nil {
			srv.handleLostConn(connId)
		}
		srv.handleMessage(connId, payload)
		srv.distributeTasks()
	}
}
