// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
)

type server struct {
	responseChan chan messageWithAddr
	writeChan    chan messageWithAddr
	conn         *lspnet.UDPConn
	clientMap    map[int]*clientInfo //int->clientId
	nextConnId   int
	params       *Params
}

type clientInfo struct {
	nextSeq        int
	bufferedMsg    map[int][]byte
	remoteAddr     lspnet.UDPAddr
	clientReadChan chan []byte
}

type messageWithAddr struct {
	message *Message
	addr    *lspnet.UDPAddr
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	newServer := &server{
		make(chan messageWithAddr),
		make(chan messageWithAddr),
		conn,
		make(map[int]*clientInfo),
		1,
		params,
	}
	go newServer.ReadRoutine()
	go newServer.MainRoutine()
	go newServer.writeRoutine()
	return newServer, nil
}

func (s *server) MainRoutine() {
	for {
		select {
		case msg := <-s.responseChan:
			switch msg.message.Type {
			case MsgConnect:
				id := s.nextConnId
				s.nextConnId++
				s.clientMap[id] = &clientInfo{
					nextSeq:        0,
					bufferedMsg:    make(map[int][]byte),
					remoteAddr:     *msg.addr,
					clientReadChan: make(chan []byte, s.params.WindowSize),
				}
				s.writeChan <- messageWithAddr{&Message{MsgAck, id, 0, 0, 0, nil}, msg.addr}
				s.clientMap[id].nextSeq++
			case MsgData:
				//todo

			case MsgAck:

			}
		}
	}

}

func (s *server) ReadRoutine() {
	for {
		payload := make([]byte, 0)
		_, addr, err := s.conn.ReadFromUDP(payload)
		if err != nil {
			fmt.Println("read routine err")
			continue
		}
		var message Message
		json.Unmarshal(payload, &message)
		s.responseChan <- messageWithAddr{message: &message, addr: addr}
	}
}

func (s *server) writeRoutine() {
	for {
		message := <-s.writeChan
		payload, err := json.Marshal(message.message)
		if err != nil {
			fmt.Println("Write routine err")
			continue
		}
		s.conn.WriteToUDP(payload, message.addr)
	}
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connId int, payload []byte) error {

	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
