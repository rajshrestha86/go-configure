package main

import (
	"google.golang.org/grpc"
	"log"
	"net"
	pb "rara/client/grpc/proto"
	"rara/server/lib"
	badger "rara/server/lib/db"
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

	db := badger.NewBadgerDB("/tmp/badger/")
	defer db.Close()

	serv := lib.Server{Db: &db, SlaveJobs: make(map[int32][]string), Jobs: make(map[int32]chan bool), JobIDCounter: 0, Running: make(map[string]int32)}
	server := grpc.NewServer()
	pb.RegisterCLIServer(server, &serv)
	err = server.Serve(lis)
	if err != nil {
		panic("Error listening on the given scanner.")
	}
	log.Println("Server started.")
}
