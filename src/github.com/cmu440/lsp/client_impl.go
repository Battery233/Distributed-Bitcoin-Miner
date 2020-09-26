// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

type client struct {
	conn        *lspnet.UDPConn
	connID      int
	outGoingSeq int
	outGoingBuf map[int]Message
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
		conn, message.ConnID, 1,make(map[int]Message),
	}
	return newClient, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
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
