package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	pb "rara/client/grpc"
	"strings"
	"time"
)

const chunkSize = 100

type RPC struct {
	Client pb.CLIClient
}

func (mg *RPC) InvokeRPC(inp string) (bool, error) {
	if mg.Client == nil {
		return false, errors.New("client hasn't been initialized")
	}

	cmd, args := mg.parseCommand(inp)

	switch cmd {
	case "slave":
		return mg.SlaveCommand(args)
	case "config":
		return mg.ConfigCommand(args)
	default:
		return false, nil
	}
}

func (mg *RPC) ConfigCommand(args []string) (bool, error) {
	if len(args) == 0 {
		return false, errors.New("invalid number of args")
	}
	switch args[0] {
	case "-f":
		//
		if len(args) < 2 {
			return false, errors.New("please supply path for the configuration file")
		}

		file, err := ioutil.ReadFile(args[1])
		if err != nil {
			return false, err
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := mg.Client.TransferConfigFile(ctx)
		if err != nil {
			panic(err)
		}

		for i := 0; i < len(file); i += chunkSize {
			index := i + chunkSize
			if index > len(file) {
				index = len(file)
			}

			err := stream.Send(&pb.Chunk{Data: file[i:index]})
			if err != nil {
				panic(err)
			}
		}

		status, err := stream.CloseAndRecv()
		fmt.Println(status)

		fmt.Println("Config file transfer.")
		return true, nil
	case "-v":
		fmt.Println("View Configuration file.")

		return true, nil
	default:
		return true, nil
	}

}

func (mg *RPC) SlaveCommand(args []string) (bool, error) {
	if len(args) == 0 {
		return false, errors.New("invalid number of args")
	}
	switch args[0] {
	case "-n":
		// Create a New Slave.
		if len(args) != 3 {
			return false, errors.New("insufficient args: -n username@host password")
		}
		userhost := strings.Split(args[1], "@")
		user := userhost[0]
		host := userhost[1]
		password := args[2]
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		r, err := mg.Client.AddSlave(ctx, &pb.Slave{Username: user, Host: host, Password: password})
		if err != nil {
			return false, err
		}
		return r.Status, err
	case "-d":
		// Delete specific slave identified by Username@Host
		if len(args) != 2 {
			return false, errors.New("please provide: username@host")
		}
		userhost := strings.Split(args[1], "@")
		user := userhost[0]
		host := userhost[1]
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		r, err := mg.Client.DeleteSlave(ctx, &pb.Slave{Username: user, Host: host, Password: ""})
		if err != nil {
			return false, err
		}
		return r.Status, err

	case "-l":
		// List Slaves.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		client, err := mg.Client.ListSlaves(ctx, &pb.Empty{})
		if err != nil {
			return false, nil
		}
		fmt.Println("List of slave machines: ")
		fmt.Println("Host\t\tUsername\t\tPassword\t\t")
		var count = 0
		for {
			slave, err := client.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return false, err
			}
			count += 1
			fmt.Println(slave.Host+"\t\t", slave.Username+"\t\t", slave.Password+"\t\t")
		}
		fmt.Println("Total Machines: ", count)
		return true, nil
	case "-Rd":
		// Recursive Delete if all the slaves.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		r, err := mg.Client.DeleteSlaves(ctx, &pb.Empty{})
		if err != nil {
			return false, err
		}
		return r.Status, err

	default:
		return false, errors.New("no action defined in cli")
	}

}

func (mg *RPC) parseCommand(input string) (string, []string) {
	var args []string
	cmd := strings.Split(input, " ")
	if len(cmd) == 1 {
		args = []string{}
	} else {
		args = cmd[1:]
	}
	return cmd[0], args

}
