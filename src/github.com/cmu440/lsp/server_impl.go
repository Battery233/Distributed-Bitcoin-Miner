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
	receivedChan           chan *messageWithAddr // channel for accepting new messages
	writeAckChan           chan *messageWithAddr //channel for ack writing
	unreadMessages         []*Message            //a slice buffer to store all ordered but unread messages
	nextUnbufferedMsgChan  chan *Message         // a channel to send a new ordered message to the buffer handling routine
	requestReadMessageChan chan struct{}         // a signal channel for the read func to request next message
	replyReadMessageChan   chan *Message         //a channel for replying message to the read func
	conn                   *lspnet.UDPConn       // the udp connection
	clientMap              map[int]*clientInfo   //a map int->clientId to store the info
	nextConnId             int                   //a int to record the id for next incoming client
	params                 *Params               //config params
}

type clientInfo struct {
	nextClientSeq int              //the seq num we expect for the next message
	nextServerSeq int              //the next seq for the message from the server to client
	bufferedMsg   map[int]*Message //message buf for this client to store unordered message k,v->seq, message
	remoteAddr    lspnet.UDPAddr   //udp address
}

type messageWithAddr struct {
	message *Message        //content
	addr    *lspnet.UDPAddr //address from the sender (will need the address to send the reply message)
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
		make([]*Message, 0),
		make(chan *Message),
		make(chan struct{}),
		make(chan *Message),
		conn,
		make(map[int]*clientInfo),
		1,
		params,
	}

	go newServer.ReadRoutine()
	go newServer.MainRoutine()
	go newServer.writeAckRoutine()
	go newServer.messageBufferRoutine()

	return newServer, nil
}

// mainRoutine mainly listens for different message types received
// from the recievedChan, and performs different tasks based on it.
func (s *server) MainRoutine() {
	for {
		msg := <-s.receivedChan
		fmt.Printf("Receive Message: %s \n", msg.message.String())
		switch msg.message.Type {
		case MsgConnect: //create a new client information and send ack
		//todo drop duplicate requests
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
				//discard messages we already received
				continue
			}
			client.bufferedMsg[seq] = msg.message
			if msg.message.Checksum != calculateCheckSum(msg.message.ConnID, msg.message.SeqNum, msg.message.Size, msg.message.Payload) {
				//discard message with wrong checksum
				continue
			}
			if msg.message.Size != len(msg.message.Payload) {
				//discard message in wrong sizes
				continue
			}
			//todo timeout
			//todo heartbeat to clients every epoch
			s.writeAckChan <- &messageWithAddr{NewAck(id, seq), msg.addr} //send the ack here
			for {
				//for all messages buffered for this client, push those with right order to the server buffer (and get
				//ready to read by the read func)
				val, exist := client.bufferedMsg[client.nextClientSeq]
				if exist {
					s.nextUnbufferedMsgChan <- val
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

// readRoutine is responsible for read messages from clients and
// send the message to the receivedChan channel and later processed
// by the mainRoutine
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

// messageBufferRoutine is responsible for responding data messages to clients.
func (s *server) messageBufferRoutine() {
	for {
		select {
		case msg := <-s.nextUnbufferedMsgChan:
			// we got another message that should go into our cache
			// and wait until the the read method is called
			s.unreadMessages = append(s.unreadMessages, msg)
		case <-s.requestReadMessageChan:
			if len(s.unreadMessages) > 0 {
				// if there's cached messages, simply send the first one back
				s.replyReadMessageChan <- s.unreadMessages[0]
				s.unreadMessages = s.unreadMessages[1:]
			} else {
				//if the unread buffer is empty, block here until next unread message arrives
				msg := <-s.nextUnbufferedMsgChan
				s.replyReadMessageChan <- msg
			}
		}
	}
}

// writeAckRoutine is responsible for writing ACK messages to the server
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

// Read sends a read request to the requestReadMessageChan channel to signal
// that we want a new message from our cache, and listen to replyReadMessageChan
// for the corresponding message value
func (s *server) Read() (int, []byte, error) {
	//todo return error properly
	s.requestReadMessageChan <- struct{}{}
	message := <-s.replyReadMessageChan
	return message.ConnID, message.Payload, nil
}

// Write writes a message payload to a client via a message with type "data" and id
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
