// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
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
	writeDataChan          chan *payloadWithId   //channel for writing outgoing data
	writeDataResultChan    chan bool             //channel for returning the result of write
}

type clientInfo struct {
	nextClientSeq            int              //the seq num we expect for the next message
	nextServerSeq            int              //the next seq for the message from the server to client
	bufferedMsg              map[int]*Message //message buf for this client to store unordered message k,v->seq, message
	remoteAddr               lspnet.UDPAddr   //udp address
	unackedBuf               map[int]*unAckedMessage
	alreadySentInEpoch       bool         //bool for showing if the message was sent during the last epoch
	alreadyHeardInEpoch      bool         //bool for showing if the heartbeat was sent during the last epoch
	lastEpochHeardFromClient int          //int for recoding last time a message is heard from the client
	outgoingBuf              []*Message   //the buffer to store messages that cannot be sent under sliding window protocol
	oldestUnackedSeq         int          //the oldest ack number we haven't received yet
	unrecordedAckBuf         map[int]bool //a map to store the acks we already received but not added to the oldestUnackedSeq yet
}

type messageWithAddr struct {
	message *Message        //content
	addr    *lspnet.UDPAddr //address from the sender (will need the address to send the reply message)
}

type payloadWithId struct {
	payload []byte
	id      int
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
		make(chan *payloadWithId),
		make(chan bool),
	}

	go newServer.readRoutine()
	go newServer.MainRoutine()
	go newServer.writeAckRoutine()
	go newServer.messageBufferRoutine()

	return newServer, nil
}

// mainRoutine mainly listens for different message types received
// from the recievedChan, and performs different tasks based on it.
func (s *server) MainRoutine() {
	ticker := time.NewTicker(time.Millisecond * time.Duration(s.params.EpochMillis))
	defer ticker.Stop()
	for {
		select {
		case msg := <-s.receivedChan:
			serverProcessMessage(s, msg)
		case writeData := <-s.writeDataChan:
			payload := writeData.payload
			connId := writeData.id
			outGoingSeq := s.clientMap[connId].nextServerSeq
			s.clientMap[connId].nextServerSeq++
			size := len(payload)
			data := NewData(connId, outGoingSeq, size, payload, calculateCheckSum(connId, outGoingSeq, size, payload))

			//send using sliding window here
			c := s.clientMap[connId]
			if len(c.unackedBuf) < s.params.MaxUnackedMessages && outGoingSeq < c.oldestUnackedSeq+s.params.WindowSize {
				c.unackedBuf[outGoingSeq] = &unAckedMessage{
					data,
					0,
					0,
				}
				payload, err := json.Marshal(data)
				if err != nil {
					s.writeDataResultChan <- false
					continue
				}
				_, err = s.conn.WriteToUDP(payload, &c.remoteAddr)
				if err != nil {
					//todo do what if the server is closed and others
					s.writeDataResultChan <- false
					continue
				}
				c.alreadySentInEpoch = true
			} else {
				c.outgoingBuf = append(c.outgoingBuf, data)
			}
			s.writeDataResultChan <- true

		case <-ticker.C:
			for id, client := range s.clientMap {
				if client.alreadySentInEpoch {
					client.alreadySentInEpoch = false
				} else {
					s.writeAckChan <- &messageWithAddr{NewAck(id, 0), &client.remoteAddr} //send the heartbeat ack here
				}

				if client.alreadyHeardInEpoch {
					client.alreadySentInEpoch = false
				} else {
					client.lastEpochHeardFromClient++
				}

				if client.lastEpochHeardFromClient == s.params.EpochLimit {
					//todo consider the client is dead
					continue
				}

				for _, element := range client.unackedBuf {
					if element.currentBackoff == element.epochCounter {
						payload, err := json.Marshal(element.message)
						if err != nil {
							continue
						}
						_, err = s.conn.WriteToUDP(payload, &client.remoteAddr)
						if err != nil {
							continue
						}
						element.epochCounter = 0
						if element.currentBackoff == 0 {
							element.currentBackoff = 1
						} else if element.currentBackoff*2 > s.params.MaxBackOffInterval {
							element.currentBackoff = s.params.MaxBackOffInterval
						} else {
							element.currentBackoff *= 2
						}
					} else {
						element.epochCounter++
					}
				}
			}
		}
	}
}

func serverProcessMessage(s *server, msg *messageWithAddr) {
	switch msg.message.Type {
	case MsgConnect: //create a new client information and send ack
		for k, v := range s.clientMap {
			if v.remoteAddr.String() == (*msg.addr).String() {
				s.writeAckChan <- &messageWithAddr{NewAck(k, 0), msg.addr}
				return
			}
		}
		id := s.nextConnId
		s.nextConnId++
		s.clientMap[id] = &clientInfo{
			0,
			1,
			make(map[int]*Message),
			*msg.addr,
			make(map[int]*unAckedMessage),
			false,
			false,
			0,
			make([]*Message, 0),
			1,
			make(map[int]bool),
		}
		s.writeAckChan <- &messageWithAddr{NewAck(id, 0), msg.addr}
		s.clientMap[id].nextClientSeq++
	case MsgData:
		id := msg.message.ConnID
		client := s.clientMap[id]
		seq := msg.message.SeqNum

		if msg.message.Size > len(msg.message.Payload) {
			fmt.Printf("Server: msg wrong size. Expected size = %d, actual size = %d\n", msg.message.Size, len(msg.message.Payload))
			//discard message in wrong sizes
			return
		} else {
			msg.message.Payload = msg.message.Payload[0:msg.message.Size]
		}

		if msg.message.Checksum != calculateCheckSum(msg.message.ConnID, msg.message.SeqNum, msg.message.Size, msg.message.Payload) {
			fmt.Printf("Server: msg wrong checksum. Expected checksum = %d, actual checksum = %d\n", msg.message.Checksum, calculateCheckSum(msg.message.ConnID, msg.message.SeqNum, msg.message.Size, msg.message.Payload))
			//discard message with wrong checksum
			return
		}

		s.writeAckChan <- &messageWithAddr{NewAck(id, seq), msg.addr} //send the ack here
		if seq < client.nextClientSeq {
			//discard messages we already received
			return
		}
		client.bufferedMsg[seq] = msg.message
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
		s.clientMap[msg.message.ConnID].lastEpochHeardFromClient = 0
		s.clientMap[msg.message.ConnID].alreadyHeardInEpoch = true
	case MsgAck:
		message := msg.message
		c := s.clientMap[msg.message.ConnID] //client
		if message.SeqNum == 0 {
			c.lastEpochHeardFromClient = 0
			c.alreadyHeardInEpoch = true
		} else {
			delete(c.unackedBuf, msg.message.SeqNum)
			if message.SeqNum >= c.oldestUnackedSeq {
				c.unrecordedAckBuf[message.SeqNum] = true
			}
			for {
				_, exist := c.unrecordedAckBuf[c.oldestUnackedSeq]
				if exist {
					delete(c.unrecordedAckBuf, c.oldestUnackedSeq)
					c.oldestUnackedSeq++
				} else {
					break
				}
			}
			for len(c.outgoingBuf) > 0 && len(c.unackedBuf) < s.params.MaxUnackedMessages && c.outgoingBuf[0].SeqNum < c.oldestUnackedSeq+s.params.WindowSize {
				data := c.outgoingBuf[0]
				c.outgoingBuf = c.outgoingBuf[1:]
				c.unackedBuf[data.SeqNum] = &unAckedMessage{
					data,
					0,
					0,
				}
				payload, err := json.Marshal(data)
				if err != nil {
					s.writeDataResultChan <- false
					continue
				}
				_, err = s.conn.WriteToUDP(payload, &c.remoteAddr)
				if err != nil {
					//todo do what if the server is closed and others
					s.writeDataResultChan <- false
					continue
				}
				c.alreadySentInEpoch = true
			}
		}
	}
}

// readRoutine is responsible for read messages from clients and
// send the message to the receivedChan channel and later processed
// by the mainRoutine
func (s *server) readRoutine() {
	for {
		payload := make([]byte, 2048)
		n, addr, err := s.conn.ReadFromUDP(payload)
		payload = payload[0:n]
		if err != nil {
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
			continue
		}
		_, err = s.conn.WriteToUDP(payload, message.addr)
		if err != nil {
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
	s.writeDataChan <- &payloadWithId{payload: payload, id: connId}
	//todo fix this channel here
	if <-s.writeDataResultChan {
		return nil
	}
	return errors.New("write error")
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
