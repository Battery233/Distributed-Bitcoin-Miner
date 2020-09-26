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
	responseChan   chan messageWithAddr
	writeChan      chan messageWithAddr
	unreadMessages chan Message
	conn           *lspnet.UDPConn
	clientMap      map[int]*clientInfo //int->clientId
	nextConnId     int
	params         *Params
}

type clientInfo struct {
	nextSeq     int
	bufferedMsg map[int]Message
	remoteAddr  lspnet.UDPAddr
}

type messageWithAddr struct {
	message *Message
	addr    *lspnet.UDPAddr
}

const maxUnreadMessageSize = 2048

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
		make(chan Message, maxUnreadMessageSize),
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
		msg := <-s.responseChan
		if msg.message.Type == MsgConnect {
			fmt.Printf("MsgConnect, content = %s\n", msg.message.String())
			id := s.nextConnId
			s.nextConnId++
			s.clientMap[id] = &clientInfo{
				nextSeq:     0,
				bufferedMsg: make(map[int]Message),
				remoteAddr:  *msg.addr,
			}
			s.writeChan <- messageWithAddr{&Message{MsgAck, id, 0, 0, 0, nil}, msg.addr}
			s.clientMap[id].nextSeq++
		} else if msg.message.Type == MsgData {
			fmt.Printf("MsgData %s\n", msg.message.String())
			id := msg.message.ConnID
			client := s.clientMap[id]
			seq := msg.message.SeqNum
			client.bufferedMsg[seq] = *msg.message
			//todo checksum
			for {
				val, exist := client.bufferedMsg[client.nextSeq]
				if exist {
					s.unreadMessages <- val
					delete(client.bufferedMsg, client.nextSeq)
					client.nextSeq++
				} else {
					break
				}
			}
		} else if msg.message.Type == MsgAck {
			fmt.Printf("MsgAsk %s\n", msg.message.String())
		}
		//todo

	}

}

func (s *server) ReadRoutine() {
	for {
		payload := make([]byte, 2048)
		n, addr, err := s.conn.ReadFromUDP(payload)
		payload = payload[0:n]
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
		fmt.Printf("reply %s\n\n", string(payload))
		s.conn.WriteToUDP(payload, message.addr)
	}
}

func (s *server) Read() (int, []byte, error) {
	message := <-s.unreadMessages
	return message.ConnID, message.Payload, nil
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
