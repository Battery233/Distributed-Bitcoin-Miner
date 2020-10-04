// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

//todo remove all prints

type client struct {
	conn                   *lspnet.UDPConn // connection object between client and server
	connID                 int
	incomingSeq            int              // the next sequence number that client should receive from server
	bufferedMsg            map[int]*Message // buffer for incoming unsorted messages
	outGoingSeq            int              // the next sequence number that client should send to server
	outGoingBuf            map[int]*Message // todo add comments here
	receivedChan           chan *Message    // channel for transferring received message object
	unreadMessages         []*Message       // cache for storing all unread messages
	nextUnbufferedMsgChan  chan *Message    // channel for transferring the message to buffer into the unreadMessages cache
	requestReadMessageChan chan struct{}    // channel for requesting to read a new message from cache
	replyReadMessageChan   chan *Message    // channel for replying the read cache request
	writeAckChan           chan *Message    // channel for replying ACK message
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
		return nil, err
	}

	// construct a new "connect" message and serialize it
	payload, err := json.Marshal(NewConnect())
	if err != nil {
		return nil, err
	}
	_, err = conn.Write(payload)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, 2048)
	//todo if ack not received after epochs
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return nil, err
	}
	// copy the buffer
	buffer = buffer[0:n]
	var message Message
	err = json.Unmarshal(buffer, &message)
	if err != nil {
		return nil, err
	}
	// should get an ACK message back from server with connID
	if message.Type != MsgAck || message.SeqNum != 0 {
		return nil, errors.New("illegal ACK for connection")
	}
	newClient := &client{
		conn,
		message.ConnID,
		1,
		make(map[int]*Message),
		1,
		make(map[int]*Message),
		make(chan *Message),
		make([]*Message, 0),
		make(chan *Message),
		make(chan struct{}),
		make(chan *Message),
		make(chan *Message),
	}

	go newClient.readRoutine()
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
// from the recievedChan, and performs different tasks based on it.
func (c *client) mainRoutine() {
	for {
		message := <-c.receivedChan
		switch message.Type {
		case MsgData:
			seq := message.SeqNum
			if seq < c.incomingSeq {
				// if the seq number from the data message received is less than
				// the expected incoming sequence number, then we are sure that
				// this is a resent message that has already been responded, therefore
				// we just ignore the message
				continue
			}
			// store message into the buffered message map, and associate the value
			// with the received sequence number
			c.bufferedMsg[seq] = message
			if message.Checksum != calculateCheckSum(message.ConnID, message.SeqNum, message.Size, message.Payload) {
				// data is corrupted, simply ignore the corrupted data
				fmt.Println("wrong checksum received")
				continue
			}
			if message.Size != len(message.Payload) {
				// size is wrong, data is corrupted, should ignore
				continue
			}
			//todo heartbeat to server every epoch

			c.writeAckChan <- NewAck(c.connID, seq)
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
		case MsgAck:
			//todo
		default:
			fmt.Println("Wrong msg type")
			continue
		}
	}
}

// readRoutine is responsible for read messages from the server and
// send the message to the receivedChan channel and later processed
// by the mainRoutine
func (c *client) readRoutine() {
	for {
		payload := make([]byte, 2048)
		n, err := c.conn.Read(payload)
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
		c.receivedChan <- &message
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
				//if the unread buffer is empty, block here until next unread message arrives
				msg := <-c.nextUnbufferedMsgChan
				c.replyReadMessageChan <- msg
			}
		}
	}
}

// writeAckRoutine is responsible for writing ACK messages to the server
func (c *client) writeAckRoutine() {
	for {
		message := <-c.writeAckChan
		payload, err := json.Marshal(message)
		if err != nil {
			fmt.Println("Write routine err")
			continue
		}
		//fmt.Printf("Reply %s\n\n", string(payload))
		_, err = c.conn.Write(payload)
		if err != nil {
			fmt.Println("Write routine err")
			continue
		}
	}
}

// Read sends a read request to the requestReadMessageChan channel to signal
// that we want a new message from our cache, and listen to replyReadMessageChan
// for the corresponding message value
func (c *client) Read() ([]byte, error) {
	//todo return error properly
	c.requestReadMessageChan <- struct{}{}
	msg := <-c.replyReadMessageChan
	return msg.Payload, nil
}

// Write writes a message payload to the server via a message with type "data"
func (c *client) Write(payload []byte) error {
	// outGoingSeq is responsible for tracking the correct sequence number
	// currently for the client to send to the server. Since we are sending
	// a new message to the server, we should increment it by 1.
	outGoingSeq := c.outGoingSeq
	c.outGoingSeq++

	//todo add buf map
	size := len(payload)
	// construct the data message
	data := NewData(c.connID, outGoingSeq, size, payload, calculateCheckSum(c.connID, outGoingSeq, size, payload))
	payload, err := json.Marshal(data)
	if err != nil {
		fmt.Println("client data marshal err")
		return err
	}
	fmt.Printf("Write data %s\n\n", data.String())
	_, err = c.conn.Write(payload)

	//todo start a timer for ack

	if err != nil {
		//todo do what if the server is closed and others
		return err
	}
	return nil
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
