package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/lsp"
)

type server struct {
	lspServer       lsp.Server
	requestMap      map[int]*requestInfo // a map to link client id and corresponding request
	requestQueue    []*requestInfo       // a list work as a queue for all requests.
	activeMiners    map[int]struct{}     // all connected miners. Use map as a set here
	availableMiners []int                // all available miners
	ongoingTasks    map[int]*taskUnit    // task units which are executing by the miners
}

type requestInfo struct {
	clientId        int         // id of the client
	data            string      // the request string data
	lower           uint64      // the lower bound of the range
	upper           uint64      // the upper bound of the range
	currentMinHash  uint64      // to store the min hash value
	currentMinNonce uint64      // to store the nonce for the current hash value
	taskUnitsLeft   uint64      // number of undone task units
	taskUnitsQueue  []*taskUnit // a list of task units to be done
}

type taskUnit struct {
	clientId int    // the id of the client
	start    uint64 // the start of the calculation
	end      uint64 // the end of the calculation
}

const chunkSize = 10000 // set default task size for a miner to 10000

func startServer(port int) (*server, error) {
	// start lsp server first
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	srv := &server{
		lspServer,
		make(map[int]*requestInfo),
		make([]*requestInfo, 0),
		make(map[int]struct{}),
		make([]int, 0),
		make(map[int]*taskUnit),
	}
	return srv, nil
}

// handleMessage can handle all type of messages the server receives
func (srv *server) handleMessage(connId int, payload []byte) {
	var message bitcoin.Message
	err := json.Unmarshal(payload, &message)
	if err != nil {
		return
	}

	switch message.Type {
	case bitcoin.Request:
		// taskUnitsLeft is the number of task units that should be distributed
		// to different miners.
		taskUnitsLeft := (message.Upper - message.Lower + 1) / chunkSize
		if (message.Upper-message.Lower+1)%chunkSize != 0 {
			// this means we have some extra task to do that has the length
			// less than chunkSize. No matter how many that is, we need another
			// miner to perform the task
			taskUnitsLeft++
		}
		// taskUnitsQueue is all the tasks corresponding to the client task request,
		// separated into smaller chunks according to the chunkSize.
		taskUnitsQueue := make([]*taskUnit, 0)
		for i := uint64(0); i < taskUnitsLeft; i++ {
			unit := &taskUnit{
				connId,
				message.Lower + chunkSize*i,
				message.Lower + chunkSize*(i+1),
			}
			if unit.end > message.Upper {
				unit.end = message.Upper
			}
			taskUnitsQueue = append(taskUnitsQueue, unit)
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
		srv.requestQueue = append(srv.requestQueue, rInfo)
		srv.requestMap[connId] = rInfo

	case bitcoin.Join:
		// activeMiners is a set recording what are all the connecting miners
		srv.activeMiners[connId] = struct{}{}
		// availableMiners are all idle miners
		srv.availableMiners = append(srv.availableMiners, connId)

	case bitcoin.Result:
		clientId := srv.ongoingTasks[connId].clientId
		rInfo, exist := srv.requestMap[clientId]

		//release the miner
		delete(srv.ongoingTasks, connId)
		srv.availableMiners = append(srv.availableMiners, connId)

		if !exist { // the client is lost and we ignore the result here
			return
		}

		// update result from miner
		rInfo.taskUnitsLeft--
		if message.Hash < rInfo.currentMinHash {
			rInfo.currentMinHash = message.Hash
			rInfo.currentMinNonce = message.Nonce
		}

		if rInfo.taskUnitsLeft == 0 { // write back the result to the client
			resultMessage := bitcoin.NewResult(rInfo.currentMinHash, rInfo.currentMinNonce)
			payload, err := json.Marshal(resultMessage)
			if err != nil {
				return
			}
			err = srv.lspServer.Write(clientId, payload)
			if err != nil {
				return
			}
			clientInfo := srv.requestMap[clientId]
			for i, e := range srv.requestQueue {
				if e == clientInfo {
					srv.requestQueue = append(srv.requestQueue[:i], srv.requestQueue[i+1:]...)
					break
				}
			}
			delete(srv.requestMap, clientId)
		}
	}
}

// handleLostConn can cleanup or reallocate tasks when a client or miner is lost
func (srv *server) handleLostConn(connId int) {
	if _, exist := srv.requestMap[connId]; exist {
		// a client is lost, remove it from the client map and request queue
		for i, e := range srv.requestQueue {
			if e.clientId == connId {
				srv.requestQueue = append(srv.requestQueue[:i], srv.requestQueue[i+1:]...)
				break
			}
		}
		delete(srv.requestMap, connId)
	} else {
		// a miner is lost
		delete(srv.activeMiners, connId) //remove it from the miner map

		// if the miner is suppose to do a task at the moment
		if _, exist := srv.ongoingTasks[connId]; exist {
			// push the lost job back to the queue
			unit := srv.ongoingTasks[connId]
			srv.requestMap[unit.clientId].taskUnitsQueue = append(srv.requestMap[unit.clientId].taskUnitsQueue, unit)
			delete(srv.ongoingTasks, connId)
		} else {
			// if the miner is suppose to be available, simply remove it
			for i, value := range srv.availableMiners {
				if value == connId {
					srv.availableMiners = append(srv.availableMiners[:i], srv.availableMiners[i+1:]...)
					break
				}
			}
		}
	}
}

// distributeTasks distributes task units to idle miners. The scheduling algorithm works as follows:
// 0. when the server receives a request from a new client, it will put the request in the requestQueue.
//    Meanwhile the server will breakup the request into small units according to the chunkSize const
//    and store them in the taskUnitsQueue of each request.
// 1. loop through all unfinished client requests.
// 2. for each ongoing client request, get the task unit queue of that client request.
// 3. check if there's any miner available. if there is, then assign the first task unit to a miner.
//    Note that miner has its own queue as well, which means we use all miners in a round-robin fashion
//    so that we distributes the work to all miners equally in the long-term.
// 4. move the request to the end of the queue.
func (srv *server) distributeTasks() {

	// calculate the total number of unfinished task units
	numUnits := 0
	for _, e := range srv.requestQueue {
		numUnits += len(e.taskUnitsQueue)
	}

	for len(srv.availableMiners) > 0 && numUnits > 0 {
		request := srv.requestQueue[0]
		// this line essentially makes the requestQueue a circle. For example, if we find
		// a client request that has non-empty taskUnitsQueue, that means there's task to
		// perform, so we assign a miner to that task. But, it is possible that not all tasks
		// are finished once we perform the previous task, therefore we need to re-push
		// the request object back into our requestQueue so that future miners can perform
		// those tasks continuously.
		srv.requestQueue = append(srv.requestQueue[1:], request)
		if len(request.taskUnitsQueue) > 0 {
			unit := request.taskUnitsQueue[0]
			request.taskUnitsQueue = request.taskUnitsQueue[1:]
			minerId := srv.availableMiners[0]
			srv.availableMiners = srv.availableMiners[1:]

			// this is the unit to be calculated
			srv.ongoingTasks[minerId] = unit
			data := srv.requestMap[unit.clientId].data

			payload, err := json.Marshal(bitcoin.NewRequest(data, unit.start, unit.end))
			if err != nil {
				return
			}
			err = srv.lspServer.Write(minerId, payload)
			if err != nil {
				// miner lost
				srv.handleLostConn(minerId)
			}
			numUnits--
		}
	}
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

	defer func() {
		if srv.lspServer != nil {
			srv.lspServer.Close()
		}
	}()

	// start to accept requests
	for {
		connId, payload, err := srv.lspServer.Read()
		if err != nil {
			srv.handleLostConn(connId)
		}
		srv.handleMessage(connId, payload)
		srv.distributeTasks()
	}
}
