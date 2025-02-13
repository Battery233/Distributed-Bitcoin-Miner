// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	conn                       *lspnet.UDPConn         // connection object between client and server
	connID                     int                     // connection id returned from Server
	incomingSeq                int                     // the next sequence number that client should receive from server
	bufferedMsg                map[int]*Message        // buffer for incoming unsorted messages
	outGoingSeq                int                     // the next sequence number that client should send to server
	unackedBuf                 map[int]*unAckedMessage // the map for storing sent but not acked data
	outgoingBuf                []*Message              // the buffer to store messages that cannot be sent under sliding window protocol
	oldestUnackedSeq           int                     // the oldest ack number we haven't received yet
	unrecordedAckBuf           map[int]bool            // a map to store the acks we already received but not added to the oldestUnackedSeq yet
	receivedChan               chan *Message           // channel for transferring received message object
	unreadMessages             []*Message              // cache for storing all unread messages
	nextUnbufferedMsgChan      chan *Message           // channel for transferring the message to buffer into the unreadMessages cache
	requestReadMessageChan     chan struct{}           // channel for requesting to read a new message from cache
	replyReadMessageChan       chan *Message           // channel for replying the read cache request
	writeAckChan               chan *Message           // channel for replying ACK message
	alreadySentInEpoch         bool                    // bool for showing if the message was sent during the last epoch
	alreadyHeardInEpoch        bool                    // bool for showing if the heartbeat was sent during the last epoch
	lastEpochHeardFromServer   int                     // int for recoding last time a message is heard from the server
	writeDataChan              chan []byte             // channel for writing outgoing data
	writeDataResultChan        chan bool               // channel for returning the result of write
	readRoutineCloseChan       chan struct{}           // channel for closing the readRoutine
	mainRoutineCloseChan       chan struct{}           // channel for closing the mainRoutine
	writeAckRoutineCloseChan   chan struct{}           // channel for closing the writeAckRouting
	messageBufRoutineCloseChan chan struct{}           // channel for closing the messageBufferRoutine
	closedSuccessfullyChan     chan struct{}           // channel for notifying all routines have been closed successfully
	isClosed                   bool                    // a flag indicating whether the Close method has been called before
	isServerLost               bool                    // a flag indicating whether the connection the Server has been lost
	serverTimeoutChan          chan struct{}           // channel for notifying the connection to the Server has been lost
	params                     *Params                 // all configuration parameters
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, err
	}

	// construct a new "connect" message and serialize it
	payload, err := json.Marshal(NewConnect())
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, err
	}

	newClient := &client{
		conn,
		0, // we haven't get the id yet, so set it to 0 at first
		1,
		make(map[int]*Message),
		1,
		make(map[int]*unAckedMessage),
		make([]*Message, 0),
		1,
		make(map[int]bool),
		make(chan *Message),
		make([]*Message, 0),
		make(chan *Message),
		make(chan struct{}),
		make(chan *Message),
		make(chan *Message),
		false,
		false,
		0,
		make(chan []byte),
		make(chan bool),
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
		false,
		false,
		make(chan struct{}),
		params,
	}

	go newClient.readRoutine()

	ticker := time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis))
	defer ticker.Stop()
	connectCounter := 1
	_, err = conn.Write(payload)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, err
	}

Connect:
	// Connect loop handles all logic related to slow start server and re-transmit the MsgConnect message.
	for {
		select {
		case <-ticker.C:
			connectCounter++
			if connectCounter > params.EpochLimit {
				// if cannot connect to the server after many epochs, close connection.
				if conn != nil {
					conn.Close()
				}
				return nil, errors.New("connect failed")
			}
			_, err = conn.Write(payload)
			if err != nil {
				if conn != nil {
					conn.Close()
				}
				return nil, err
			}
		case msg := <-newClient.receivedChan:
			// heard back from server
			if msg.Type != MsgAck || msg.SeqNum != 0 {
				break
			}
			newClient.connID = msg.ConnID
			break Connect
		}
	}

	go newClient.mainRoutine()
	go newClient.writeAckRoutine()
	go newClient.messageBufferRoutine()

	return newClient, nil
}

// ConnID returns the connection ID associated with the client
func (c *client) ConnID() int {
	return c.connID
}

// mainRoutine mainly listens for different message types received
// from the receivedChan, and performs different tasks based on it.
func (c *client) mainRoutine() {
	// ticker is used for firing events during each epoch
	ticker := time.NewTicker(time.Millisecond * time.Duration(c.params.EpochMillis))
	defer ticker.Stop()
	closed := false
	for {
		allBufClearedBeforeClose := false
		select {
		case <-c.mainRoutineCloseChan:
			// signal other cases, wait until all buffered messages have been sent and read, then terminate the routine
			closed = true
			if len(c.outgoingBuf) == 0 && len(c.unackedBuf) == 0 {
				allBufClearedBeforeClose = true
			}
		case message := <-c.receivedChan:
			allBufClearedBeforeClose = clientProcessMessage(c, message, closed)
		case payload := <-c.writeDataChan:
			if closed {
				// if connection is closed and we get further write messages, simply ignore
				c.writeDataResultChan <- false
				continue
			}
			// outGoingSeq is responsible for tracking the correct sequence number
			// currently for the client to send to the server. Since we are sending
			// a new message to the server, we should increment it by 1.
			outGoingSeq := c.outGoingSeq
			c.outGoingSeq++
			size := len(payload)
			// construct the data message
			data := NewData(c.connID, outGoingSeq, size, payload, calculateCheckSum(c.connID, outGoingSeq, size, payload))
			if len(c.unackedBuf) < c.params.MaxUnackedMessages && outGoingSeq < c.oldestUnackedSeq+c.params.WindowSize {
				c.unackedBuf[outGoingSeq] = &unAckedMessage{
					data,
					0,
					0,
				}
				payload, err1 := json.Marshal(data)
				_, err2 := c.conn.Write(payload)
				if err1 != nil || err2 != nil {
					c.writeDataResultChan <- false
					continue
				}
				c.alreadySentInEpoch = true
			} else {
				c.outgoingBuf = append(c.outgoingBuf, data)
			}
			c.writeDataResultChan <- true

		case <-ticker.C:
			// epoch case
			if c.alreadySentInEpoch {
				c.alreadySentInEpoch = false
			} else {
				c.writeAckChan <- NewAck(c.connID, 0) // heart beat is sent here
			}

			if !c.alreadyHeardInEpoch {
				c.lastEpochHeardFromServer++
			}
			c.alreadyHeardInEpoch = false
			if c.lastEpochHeardFromServer == c.params.EpochLimit {
				// if we haven't heard back from server in params.EpochLimit epochs, signal the timeout channel
				// and terminate the connection
				c.serverTimeoutChan <- struct{}{}
				break
			}

			for _, element := range c.unackedBuf {
				if element.currentBackoff == element.epochCounter { // if backoff has reached, time to send the message again
					payload, err := json.Marshal(element.message)
					if err != nil {
						continue
					}
					_, err = c.conn.Write(payload)
					if err != nil {
						continue
					}
					element.epochCounter = 0
					if element.currentBackoff == 0 {
						element.currentBackoff = 1
					} else if element.currentBackoff*2 > c.params.MaxBackOffInterval {
						element.currentBackoff = c.params.MaxBackOffInterval
					} else {
						element.currentBackoff *= 2
					}
				} else { // otherwise, simply increment the backoff counter
					element.epochCounter++
				}
			}
		}

		if allBufClearedBeforeClose { //time to close everything
			c.conn.Close()
			c.messageBufRoutineCloseChan <- struct{}{}
			close(c.receivedChan)
			c.readRoutineCloseChan <- struct{}{}
			close(c.writeAckChan)
			c.writeAckRoutineCloseChan <- struct{}{}
			c.closedSuccessfullyChan <- struct{}{}
			return
		}
	}
}

// clientProcessMessage handles all message received from the server.
// this method returns true if the Close function is called and buffers have been cleared, and returns false otherwise.
func clientProcessMessage(c *client, message *Message, closed bool) bool {
	switch message.Type {
	case MsgData:
		if closed {
			//ignore data message
			return false
		}
		seq := message.SeqNum

		if message.Size > len(message.Payload) {
			// size is wrong, data is corrupted, should ignore
			return false
		} else {
			// trim message
			message.Payload = message.Payload[0:message.Size]
		}

		//if message.Checksum != calculateCheckSum(message.ConnID, message.SeqNum, message.Size, message.Payload) {
		//	// data is corrupted, simply ignore the corrupted data
		//	return false
		//}

		c.writeAckChan <- NewAck(c.connID, seq)
		if seq < c.incomingSeq {
			// if the seq number from the data message received is less than
			// the expected incoming sequence number, then we are sure that
			// this is a resent message that has already been responded, therefore
			// we just ignore the message
			return false
		}
		// store message into the buffered message map, and associate the value
		// with the received sequence number
		c.bufferedMsg[seq] = message
		// otherwise, we send messages back to the server one by one
		// by incrementing the c.incomingSeq number
		for {
			val, exist := c.bufferedMsg[c.incomingSeq]
			if exist {
				c.nextUnbufferedMsgChan <- val
				delete(c.bufferedMsg, c.incomingSeq)
				c.incomingSeq++
			} else {
				break
			}
		}
		c.lastEpochHeardFromServer = 0 // reset this because we already heard back from server in this epoch
		c.alreadyHeardInEpoch = true
	case MsgAck:
		if message.SeqNum == 0 { // if it is a heartbeat message
			c.lastEpochHeardFromServer = 0
			c.alreadyHeardInEpoch = true
		} else { // ack for a MsgData message
			delete(c.unackedBuf, message.SeqNum)
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

			for len(c.outgoingBuf) > 0 &&
				len(c.unackedBuf) < c.params.MaxUnackedMessages &&
				c.outgoingBuf[0].SeqNum < c.oldestUnackedSeq+c.params.WindowSize {
				data := c.outgoingBuf[0]
				c.outgoingBuf = c.outgoingBuf[1:]
				c.unackedBuf[data.SeqNum] = &unAckedMessage{
					data,
					0,
					0,
				}
				payload, _ := json.Marshal(data)
				c.conn.Write(payload)
				c.alreadySentInEpoch = true
			}
		}
	default:
		return false
	}
	return len(c.outgoingBuf) == 0 && len(c.unackedBuf) == 0 && closed
}

// readRoutine is responsible for read messages from the server and
// send the message to the receivedChan channel and later processed
// by the mainRoutine
func (c *client) readRoutine() {
	// we close this routine by calling `close` to the receivedChan channel, but this routine
	// will attempt to write to that channel, therefore there will be a panic. By handling
	// the panic we terminate the routine.
	defer func() {
		if a := recover(); a != nil {
			<-c.readRoutineCloseChan
		}
	}()

	for {
		select {
		case <-c.readRoutineCloseChan:
			return
		default:
			payload := make([]byte, 2048)
			n, err := c.conn.Read(payload)
			if err != nil {
				continue
			}
			payload = payload[0:n]
			var message Message
			err = json.Unmarshal(payload, &message)
			if err != nil {
				continue
			}
			c.receivedChan <- &message
		}
	}
}

// messageBufferRoutine is responsible for responding data messages to the server.
func (c *client) messageBufferRoutine() {
	for {
		select {
		case msg := <-c.nextUnbufferedMsgChan:
			// we got another message that should go into our cache
			// and wait until the read method is called
			c.unreadMessages = append(c.unreadMessages, msg)
		case <-c.requestReadMessageChan:
			// the read method is called, and we should first
			// check if we have buffered messages in our cache
			if len(c.unreadMessages) > 0 {
				// if there's cached messages, simply send the first one back
				c.replyReadMessageChan <- c.unreadMessages[0]
				c.unreadMessages = c.unreadMessages[1:]
			} else {
				// if the unread buffer is empty, block here until next unread message arrives
				select {
				case msg := <-c.nextUnbufferedMsgChan:
					c.replyReadMessageChan <- msg
				case <-c.serverTimeoutChan:
					c.replyReadMessageChan <- &Message{}
				case <-c.messageBufRoutineCloseChan:
					return
				}
			}
		case <-c.messageBufRoutineCloseChan:
			return
		}
	}
}

// writeAckRoutine is responsible for writing ACK messages to the server
func (c *client) writeAckRoutine() {
	for {
		select {
		case <-c.writeAckRoutineCloseChan:
			return
		default:
			message, ok := <-c.writeAckChan
			if !ok {
				continue
			}
			payload, err := json.Marshal(message)
			if err != nil {
				continue
			}
			_, err = c.conn.Write(payload)
			if err != nil {
				continue
			}
		}
	}
}

// Read sends a read request to the requestReadMessageChan channel to signal
// that we want a new message from our cache, and listen to replyReadMessageChan
// for the corresponding message value
func (c *client) Read() ([]byte, error) {
	if c.isClosed {
		return nil, errors.New("client has been already closed")
	}
	c.requestReadMessageChan <- struct{}{}
	msg := <-c.replyReadMessageChan
	if msg.Payload != nil {
		return msg.Payload, nil
	} else {
		c.isServerLost = true
		return nil, errors.New("the server is lost")
	}
}

// Write writes a message payload to the server via a message with type "data"
func (c *client) Write(payload []byte) error {
	if c.isServerLost {
		return errors.New("the server is lost")
	} else {
		c.writeDataChan <- payload
		if <-c.writeDataResultChan {
			return nil
		} else {
			return errors.New("write data to server error")
		}
	}
}

// close the client. Return error if the client is already closed
func (c *client) Close() error {
	if c.isClosed {
		return errors.New("client has been already closed")
	}
	c.mainRoutineCloseChan <- struct{}{}
	close(c.mainRoutineCloseChan)
	<-c.closedSuccessfullyChan
	c.isClosed = true
	return nil
}
