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
	conn           *lspnet.UDPConn
	connID         int
	incomingSeq    int
	bufferedMsg    map[int]*Message //buffer for incoming unsorted messages
	outGoingSeq    int
	outGoingBuf    map[int]*Message
	receivedChan   chan *Message
	unreadMessages chan *Message
	writeAckChan   chan *Message
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
	buffer = buffer[0:n]
	var message Message
	err = json.Unmarshal(buffer, &message)
	if err != nil {
		return nil, err
	}
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
		make(chan *Message, maxUnreadMessageSize),
		make(chan *Message),
	}

	go newClient.readRoutine()
	go newClient.mainRoutine()
	go newClient.writeAckRoutine()

	return newClient, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) mainRoutine() {
	for {
		message := <-c.receivedChan
		switch message.Type {
		case MsgData:
			seq := message.SeqNum
			if seq < c.incomingSeq {
				continue
			}
			c.bufferedMsg[seq] = message
			if message.Checksum != calculateCheckSum(message.ConnID, message.SeqNum, message.Size, message.Payload) {
				fmt.Println("wrong checksum received")
				continue
			}
			if message.Size!=len(message.Payload) {
				continue
			}
			//todo heartbeat to server every epoch
			c.writeAckChan <- NewAck(c.connID, seq)
			for {
				val, exist := c.bufferedMsg[c.incomingSeq]
				if exist {
					c.unreadMessages <- val
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

func (c *client) Read() ([]byte, error) {
	//todo return error properly
	msg := <-c.unreadMessages
	return msg.Payload, nil
}

func (c *client) Write(payload []byte) error {
	outGoingSeq := c.outGoingSeq
	c.outGoingSeq++

	//todo add buf map
	size := len(payload)
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
