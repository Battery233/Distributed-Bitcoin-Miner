package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"os"

	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	miner, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		return nil, err
	}

	joinRequest := bitcoin.NewJoin()
	payload, err := json.Marshal(joinRequest)
	if err != nil {
		return nil, err
	}

	err = miner.Write(payload)
	if err != nil {
		return nil, err
	}
	return miner, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)

	defer func() () {
		if miner != nil {
			miner.Close()
		}
	}()

	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	for {
		response, err := miner.Read()
		if err != nil {
			break
		}
		var message bitcoin.Message
		err = json.Unmarshal(response, &message)
		if err != nil {
			break
		}
		data := message.Data
		lower := message.Lower
		upper := message.Upper
		hash, nonce := calculateHashAndNonce(data, lower, upper)
		result := bitcoin.NewResult(hash, nonce)
		resultPayload, err := json.Marshal(result)
		if err != nil {
			break
		}
		err = miner.Write(resultPayload)
		if err != nil {
			break
		}
	}
}

func calculateHashAndNonce(data string, lower, upper uint64) (uint64, uint64) {
	min := bitcoin.Hash(data, upper)
	nonce := upper
	for i := lower; i < upper; i++ {
		hash := bitcoin.Hash(data, i)
		if hash < min {
			min = hash
			nonce = i
		}
	}
	return min, nonce
}
