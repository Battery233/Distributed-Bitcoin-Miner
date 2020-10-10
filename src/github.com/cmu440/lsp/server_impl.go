// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type server struct {
	receivedChan               chan *messageWithAddr // channel for accepting new messages
	writeAckChan               chan *messageWithAddr // channel for ack writing
	unreadMessages             []*Message            // slice buffer to store all ordered but unread messages
	nextUnbufferedMsgChan      chan *Message         // channel to send a new ordered message to the buffer handling routine
	requestReadMessageChan     chan struct{}         // signal channel for the read func to request next message
	replyReadMessageChan       chan *Message         // channel for replying message to the read func
	conn                       *lspnet.UDPConn       // the udp connection
	clientMap                  map[int]*clientInfo   // map int->clientId to store the info
	nextConnId                 int                   // int to record the id for next incoming client
	params                     *Params               // config params
	writeDataChan              chan *payloadWithId   // channel for writing outgoing data
	writeDataResultChan        chan bool             // channel for returning the result of write
	closeConnChan              chan int              // channel for signalling to close a connection with a connID
	closeConnReplyChan         chan bool             // channel for signalling whether the close was successful
	isClosed                   bool                  // flag indicating whether the Close method has been called before
	closeServerSignalChan      chan struct{}         // channel for closing the server
	readRoutineCloseChan       chan struct{}         // channel for closing the readRoutine
	writeAckRoutineCloseChan   chan struct{}         // channel for closing the writeAckRoutine
	messageBufRoutineCloseChan chan struct{}         // channel for closing the messageBufferRoutine
	closeServerSuccessChan     chan bool             // channel for signalling whether Close was successful
}

type clientInfo struct {
	nextClientSeq            int                     // seq num we expect for the next message
	nextServerSeq            int                     // next seq for the message from the server to client
	bufferedMsg              map[int]*Message        // message buf for this client to store unordered message k,v->seq, message
	remoteAddr               lspnet.UDPAddr          // udp address
	unackedBuf               map[int]*unAckedMessage // buffer for storing all messages that have been sent by waiting for ack
	alreadySentInEpoch       bool                    // bool for showing if the message was sent during the last epoch
	alreadyHeardInEpoch      bool                    // bool for showing if the heartbeat was sent during the last epoch
	lastEpochHeardFromClient int                     // int for recoding last time a message is heard from the client
	outgoingBuf              []*Message              // buffer to store messages that cannot be sent under sliding window protocol
	oldestUnackedSeq         int                     // oldest ack number we haven't received yet
	unrecordedAckBuf         map[int]bool            // map to store the acks we already received but not added to the oldestUnackedSeq yet
	isClosed                 bool                    // is the client supposed to be closed
}

type messageWithAddr struct {
	message *Message        // content
	addr    *lspnet.UDPAddr // address from the sender (will need the address to send the reply message)
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
		make(chan int),
		make(chan bool),
		false,
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
		make(chan bool),
	}

	go newServer.readRoutine()
	go newServer.mainRoutine()
	go newServer.writeAckRoutine()
	go newServer.messageBufferRoutine()

	return newServer, nil
}

// mainRoutine mainly listens for different message types received
// from the receivedChan, and performs different tasks based on it.
func (s *server) mainRoutine() {
	// ticker for handling epoch event
	ticker := time.NewTicker(time.Millisecond * time.Duration(s.params.EpochMillis))
	defer ticker.Stop()
	serverClosed := false
	serverClosedSuccess := true
	for {
		select {
		case msg := <-s.receivedChan:
			serverProcessMessage(s, msg)

		case connId := <-s.closeConnChan: //case of closing a client connection
			info := s.clientMap[connId]
			if info == nil {
				s.closeConnReplyChan <- false
			} else {
				s.closeConnReplyChan <- true
			}
			if len(info.unackedBuf) == 0 && len(info.outgoingBuf) == 0 {
				//remove the client only if all buffered messages are sent and acked
				delete(s.clientMap, connId)
			} else {
				info.isClosed = true
			}

		case writeData := <-s.writeDataChan:
			if s.clientMap[writeData.id] == nil {
				// if the connection has already been lost, simply ignore the payload
				s.writeDataResultChan <- false
				continue
			}
			payload := writeData.payload
			connId := writeData.id
			outGoingSeq := s.clientMap[connId].nextServerSeq
			s.clientMap[connId].nextServerSeq++
			size := len(payload)
			// construct the new data message
			data := NewData(connId, outGoingSeq, size, payload, calculateCheckSum(connId, outGoingSeq, size, payload))

			//send using sliding window here
			c := s.clientMap[connId]
			if len(c.unackedBuf) < s.params.MaxUnackedMessages && outGoingSeq < c.oldestUnackedSeq+s.params.WindowSize {
				c.unackedBuf[outGoingSeq] = &unAckedMessage{
					data,
					0,
					0,
				}
				payload, err1 := json.Marshal(data)
				_, err2 := s.conn.WriteToUDP(payload, &c.remoteAddr)
				if err1 != nil || err2 != nil {
					s.writeDataResultChan <- false
					continue
				}
				c.alreadySentInEpoch = true
			} else {
				// too many messages unacked, put message to buffer (sliding window congestion control)
				c.outgoingBuf = append(c.outgoingBuf, data)
			}
			s.writeDataResultChan <- true

		case <-ticker.C:
			// epoch case
			if serverClosed && len(s.clientMap) == 0 { //if the server is ready to shutdown
				s.conn.Close()
				s.messageBufRoutineCloseChan <- struct{}{}
				close(s.receivedChan)
				s.readRoutineCloseChan <- struct{}{}
				close(s.writeAckChan)
				s.writeAckRoutineCloseChan <- struct{}{}
				s.closeServerSuccessChan <- serverClosedSuccess
				return
			}

			for id, client := range s.clientMap {
				if client.alreadySentInEpoch {
					client.alreadySentInEpoch = false
				} else {
					// send heartbeat here
					s.writeAckChan <- &messageWithAddr{NewAck(id, 0), &client.remoteAddr}
				}
				if !client.alreadyHeardInEpoch {
					client.lastEpochHeardFromClient++
				}
				client.alreadyHeardInEpoch = false

				if client.lastEpochHeardFromClient == s.params.EpochLimit {
					delete(s.clientMap, id)
					// we send an message with nil payload to indicate that the client is lost
					s.nextUnbufferedMsgChan <- &Message{
						ConnID: id,
					}
					if serverClosed {
						serverClosedSuccess = false
					}
					continue
				}

				for _, element := range client.unackedBuf {
					if element.currentBackoff == element.epochCounter { // if backoff has reached, time to send the message again
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
					} else { // otherwise, simply increment the backoff counter
						element.epochCounter++
					}
				}
			}

		case <-s.closeServerSignalChan: //case of getting signal from the server Close() call
			serverClosed = true //set the flag here. Closing check will happen at the next epoch
			for connId, info := range s.clientMap {
				//closing all connections to the clients first
				if len(info.unackedBuf) == 0 && len(info.outgoingBuf) == 0 {
					delete(s.clientMap, connId)
				} else {
					info.isClosed = true
				}
			}
		}
	}
}

// serverProcessMessage handles all message received from the client.
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
			false,
		}
		s.writeAckChan <- &messageWithAddr{NewAck(id, 0), msg.addr}
		s.clientMap[id].nextClientSeq++
	case MsgData:
		id := msg.message.ConnID
		client := s.clientMap[id]
		seq := msg.message.SeqNum

		if msg.message.Size > len(msg.message.Payload) {
			//discard message in wrong sizes
			return
		} else { //discard extra bytes
			msg.message.Payload = msg.message.Payload[0:msg.message.Size]
		}

		if msg.message.Checksum != calculateCheckSum(msg.message.ConnID, msg.message.SeqNum, msg.message.Size, msg.message.Payload) {
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
		if c == nil {
			return
		}
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
			//move the sliding window here
			for len(c.outgoingBuf) > 0 && len(c.unackedBuf) < s.params.MaxUnackedMessages && c.outgoingBuf[0].SeqNum < c.oldestUnackedSeq+s.params.WindowSize {
				data := c.outgoingBuf[0]
				c.outgoingBuf = c.outgoingBuf[1:]
				c.unackedBuf[data.SeqNum] = &unAckedMessage{
					data,
					0,
					0,
				}
				payload, _ := json.Marshal(data)
				s.conn.WriteToUDP(payload, &c.remoteAddr)
				c.alreadySentInEpoch = true
			}
		}
	}

	info := s.clientMap[msg.message.ConnID]
	if info != nil && info.isClosed &&
		len(info.outgoingBuf) == 0 &&
		len(info.unackedBuf) == 0 {
		// remove the client from map if the client should be disconnected and messages are sent and acked
		delete(s.clientMap, msg.message.ConnID)
	}
}

// readRoutine is responsible for read messages from clients and
// send the message to the receivedChan channel and later processed
// by the mainRoutine
func (s *server) readRoutine() {
	defer func() {
		if a := recover(); a != nil {
			<-s.readRoutineCloseChan
		}
	}()

	for {
		select {
		case <-s.readRoutineCloseChan:
			return
		default:
			payload := make([]byte, 2048)
			n, addr, err := s.conn.ReadFromUDP(payload)
			if err != nil {
				continue
			}
			payload = payload[0:n]
			var message Message
			err = json.Unmarshal(payload, &message)
			if err != nil {
				continue
			}
			s.receivedChan <- &messageWithAddr{message: &message, addr: addr}
		}
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
				select {
				case <-s.messageBufRoutineCloseChan:
					return
				case msg := <-s.nextUnbufferedMsgChan:
					s.replyReadMessageChan <- msg
				}
			}
		case <-s.messageBufRoutineCloseChan:
			return
		}
	}
}

// writeAckRoutine is responsible for writing ACK messages to the server
func (s *server) writeAckRoutine() {
	for {
		select {
		case <-s.writeAckRoutineCloseChan:
			return
		default:
			message, ok := <-s.writeAckChan
			if !ok {
				continue
			}
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
}

// Read sends a read request to the requestReadMessageChan channel to signal
// that we want a new message from our cache, and listen to replyReadMessageChan
// for the corresponding message value
func (s *server) Read() (int, []byte, error) {
	if s.isClosed {
		return 0, nil, errors.New("server already closed")
	}
	s.requestReadMessageChan <- struct{}{}
	message := <-s.replyReadMessageChan
	if message.Payload != nil {
		return message.ConnID, message.Payload, nil
	} else {
		return message.ConnID, nil, errors.New("one client dropped")
	}
}

// Write writes a message payload to a client via a message with type "data" and id
func (s *server) Write(connId int, payload []byte) error {
	if s.isClosed {
		return errors.New("server already closed")
	}
	s.writeDataChan <- &payloadWithId{payload: payload, id: connId}
	if <-s.writeDataResultChan {
		return nil
	} else {
		return errors.New("write error")
	}
}

//a non-blocking call to close a connection
func (s *server) CloseConn(connId int) error {
	if s.isClosed {
		return errors.New("the server has been already closed")
	}
	s.closeConnChan <- connId
	if !<-s.closeConnReplyChan {
		return errors.New("channel does not exist")
	}
	return nil
}

//close the server
func (s *server) Close() error {
	s.isClosed = true
	s.closeServerSignalChan <- struct{}{}
	if <-s.closeServerSuccessChan {
		return nil
	} else {
		return errors.New("client lost during closing")
	}
}
