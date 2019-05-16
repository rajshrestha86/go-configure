package main

import (
	"google.golang.org/grpc"
	"log"
	"net"
	main2 "rara/client"
	"rara/server/lib"
)

const (
	port = ":50000"
)

func main() {
	var err error
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("failed to listen : ", err)
	}

	db := lib.NewBadgerDB("/tmp/badger/")
	defer db.Close()

	serv := lib.Server{Db: &db}
	server := grpc.NewServer()
	main2.RegisterCLIServer(server, &serv)

	err = server.Serve(lis)
	if err != nil {
		panic("Error listening on the given scanner.")
	}
}
