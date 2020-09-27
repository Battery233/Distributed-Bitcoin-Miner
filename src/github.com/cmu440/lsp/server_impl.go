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
	receivedChan   chan *messageWithAddr
	writeAckChan   chan *messageWithAddr
	unreadMessages chan *Message
	conn           *lspnet.UDPConn
	clientMap      map[int]*clientInfo //int->clientId
	nextConnId     int
	params         *Params
}

type clientInfo struct {
	nextClientSeq int
	nextServerSeq int
	bufferedMsg   map[int]*Message
	remoteAddr    lspnet.UDPAddr
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
		make(chan *messageWithAddr),
		make(chan *messageWithAddr),
		//todo remove it later
		make(chan *Message, maxUnreadMessageSize),
		conn,
		make(map[int]*clientInfo),
		1,
		params,
	}
	go newServer.ReadRoutine()
	go newServer.MainRoutine()
	go newServer.writeAckRoutine()
	return newServer, nil
}

func (s *server) MainRoutine() {
	for {
		msg := <-s.receivedChan
		fmt.Printf("Receive Message: %s \n", msg.message.String())
		switch msg.message.Type {
		case MsgConnect:
			id := s.nextConnId
			s.nextConnId++
			s.clientMap[id] = &clientInfo{
				nextClientSeq: 0,
				bufferedMsg:   make(map[int]*Message),
				remoteAddr:    *msg.addr,
				nextServerSeq: 1,
			}
			s.writeAckChan <- &messageWithAddr{NewAck(id, 0), msg.addr}
			s.clientMap[id].nextClientSeq++
		case MsgData:
			id := msg.message.ConnID
			client := s.clientMap[id]
			seq := msg.message.SeqNum
			if seq < client.nextClientSeq {
				continue
			}
			client.bufferedMsg[seq] = msg.message
			if msg.message.Checksum != calculateCheckSum(msg.message.ConnID, msg.message.SeqNum, msg.message.Size, msg.message.Payload) {
				continue
			}
			if msg.message.Size != len(msg.message.Payload) {
				continue
			}
			//todo timeout
			//todo heartbeat to clients every epoch
			s.writeAckChan <- &messageWithAddr{NewAck(id, seq), msg.addr}
			for {
				val, exist := client.bufferedMsg[client.nextClientSeq]
				if exist {
					s.unreadMessages <- val
					delete(client.bufferedMsg, client.nextClientSeq)
					client.nextClientSeq++
				} else {
					break
				}
			}
		case MsgAck:
			//todo
		}
	}
}

func (s *server) ReadRoutine() {
	for {
		payload := make([]byte, 2048)
		n, addr, err := s.conn.ReadFromUDP(payload)
		payload = payload[0:n]
		if err != nil {
			fmt.Println("Read routine err")
			continue
		}
		var message Message
		err = json.Unmarshal(payload, &message)
		if err != nil {
			continue
		}
		s.receivedChan <- &messageWithAddr{message: &message, addr: addr}
	}
}

func (s *server) writeAckRoutine() {
	for {
		message := <-s.writeAckChan
		payload, err := json.Marshal(message.message)
		if err != nil {
			fmt.Println("Write routine err")
			continue
		}
		fmt.Printf("Reply %s\n\n", string(payload))
		_, err = s.conn.WriteToUDP(payload, message.addr)
		if err != nil {
			fmt.Println("Write routine err")
			continue
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	//todo return error properly
	message := <-s.unreadMessages
	return message.ConnID, message.Payload, nil
}

func (s *server) Write(connId int, payload []byte) error {
	outGoingSeq := s.clientMap[connId].nextServerSeq
	s.clientMap[connId].nextServerSeq++
	size := len(payload)
	data := NewData(connId, outGoingSeq, size, payload, calculateCheckSum(connId, outGoingSeq, size, payload))
	payload, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Write routine err")
		return err
	}
	fmt.Printf("Write data %s\n\n", data.String())
	_, err = s.conn.WriteToUDP(payload, &s.clientMap[connId].remoteAddr)
	if err != nil {
		//todo do what if the client is closed and others
		return err
	}
	return nil
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
