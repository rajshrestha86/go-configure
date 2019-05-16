package main

import (
	"bufio"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"os"
	pb "rara/client/grpc"
	"rara/client/grpc/manager"
)

const (
	address = "localhost:50000"
)

// Commands Used


func main() {
	//	Set up a connection to the server.

	var reader *bufio.Reader
	var dir string
	var inputCmd []byte
	var err error
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic("Did not connect. ")
	}
	defer conn.Close()

	client := pb.NewCLIClient(conn)
	RPCManager := manager.RPC{Client: client}
	// Infinite loop get command and process it.
	for {
		reader = bufio.NewReader(os.Stdin)
		dir, err = os.Getwd()
		if err != nil {
			panic(err)
		}

		fmt.Print("raracli (" + dir + ")-> ")
		inputCmd, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Exited main loop.")
				break
			} else {
				panic(err)
			}
		}

		_, err := RPCManager.InvokeRPC(string(inputCmd))
		if err != nil {
			fmt.Println(err)
		}
	}
}
