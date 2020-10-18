package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"os"
	"strconv"

	"github.com/cmu440/lsp"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer func() () {
		if client != nil {
			client.Close()
		}
	}()

	request := bitcoin.NewRequest(message, 0, maxNonce)
	payload, err := json.Marshal(request)
	if err != nil {
		fmt.Println("Message json encoding error", err)
		return
	}

	err = client.Write(payload)
	if err != nil {
		printDisconnected()
		return
	}

	response, err := client.Read()

	if err != nil {
		printDisconnected()
		return
	}
	var result bitcoin.Message
	if json.Unmarshal(response[:], &result) != nil {
		printDisconnected()
		return
	}
	printResult(result.Hash, result.Nonce)
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
