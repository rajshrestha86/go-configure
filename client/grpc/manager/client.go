package manager

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/fatih/color"
	"google.golang.org/grpc/status"
	"io"
	"io/ioutil"
	"os"
	pb "rara/client/grpc/proto"
	"strconv"
	"strings"
	"time"
	//"github.com/fatih/color"
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
	case "listen":
		return mg.ListenCommand(args)
	case "job":
		return mg.JobsCommand(args)
	case "output":
		return mg.OutputCommand(args)
	default:
		return false, nil
	}
}

func (mg *RPC) OutputCommand(args []string) (bool, error) {
	if len(args) < 1 {
		return false, errors.New("insufficient args")
	}
	switch args[0] {
	case "-list":
		if len(args) < 2 {
			return false, errors.New("insufficient args")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client, err := mg.Client.ListOutputKeysForHost(ctx, &pb.Slave{Id: args[1]})
		if err != nil {
			return false, err
		}

		color.Set(color.FgHiGreen)
		defer color.Unset()
		for {
			value, err := client.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return false, err
			}

			fmt.Println(value.Text)
		}
		err = client.CloseSend()

		// Returns true or false depending on error.
		return err == nil, err
	case "-id":
		if len(args) < 4 {
			return false, errors.New("insufficient args")
		}
		outputId := &pb.OutputId{Id: args[1]}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if args[3] == "raw" {
			client, err := mg.Client.GetRawOutput(ctx, outputId)
			if err != nil {
				return false, err
			}
			color.Set(color.FgHiCyan)
			defer color.Unset()
			for {
				value, err := client.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return false, err
				}

				fmt.Println(value.Text)
			}
			err = client.CloseSend()
			return err == nil, err

		} else if args[3] == "matched" {
			client, err := mg.Client.GetMatchedOutput(ctx, outputId)
			if err != nil {
				return false, err
			}
			color.Set(color.FgHiGreen)
			defer color.Unset()
			for {
				value, err := client.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return false, err
				}
				fmt.Println(value.Text)
			}

			err = client.CloseSend()
			return err == nil, err
		} else {
			return false, errors.New("invalid outputType")
		}
	case "-rm":
		if len(args) < 3 {
			return false, errors.New("insufficient args")
		}
		switch args[1] {
		case "-id":
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			response, err := mg.Client.DeleteOutput(ctx, &pb.OutputId{Id: args[2]})
			if err != nil {
				st := status.Convert(err)
				return false, errors.New(st.Message())
			}
			return response.Status, err
		case "-timestamp":
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			response, err := mg.Client.DeleteOutputWithGivenTimestamp(ctx, &pb.OutputId{Id: args[2]})
			if err != nil {
				st := status.Convert(err)
				return false, errors.New(st.Message())
			}
			return response.Status, err
		case "-all":
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			response, err := mg.Client.DeleteAllOutputOfHost(ctx, &pb.Slave{Id: args[2]})
			if err != nil {
				st := status.Convert(err)
				return false, errors.New(st.Message())
			}
			return response.Status, err

		}
	default:
		return false, nil
	}
	return false, nil
}

func (mg *RPC) JobsCommand(args []string) (bool, error) {
	if len(args) == 0 {
		return false, errors.New("insufficient args")
	}

	switch args[0] {
	case "-list":
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client, err := mg.Client.ViewAllRunningJobs(ctx, &pb.Empty{})
		if err != nil {
			return false, err
		}

		color.Set(color.FgHiCyan)
		defer color.Unset()
		for {
			value, err := client.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return false, err
			}

			fmt.Println(value.Text)
		}

		err = client.CloseSend()

		// Returns true or false depending on error.
		return err == nil, err
	case "stop":
		if len(args) < 2 {
			return false, errors.New("insufficient args")
		}
		switch args[1] {
		case "-id":
			if len(args) < 3 {
				return false, errors.New("insufficient args")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			jobId, err := strconv.Atoi(args[2])
			if err != nil {
				return false, errors.New("enter proper type of jobID")
			}
			status, err := mg.Client.StopListeningJob(ctx, &pb.Job{Id: int32(jobId)})
			if err != nil {
				return false, err
			}

			return status.Status, err
		case "-all":
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := mg.Client.StopAllRunningJobs(ctx, &pb.Empty{})

			if err != nil {
				st := status.Convert(err)
				return false, errors.New(st.Message())
			} else {
				return true, nil
			}
		default:
			return false, errors.New("no action defined")
		}
	default:
		return true, nil
	}

}

func (mg *RPC) ListenCommand(args []string) (bool, error) {
	if len(args) == 0 {
		return false, errors.New("insufficient args")
	}

	switch args[0] {
	case "-file":
		if len(args) < 2 {
			return false, errors.New("insufficient args")
		}

		file, err := os.Open(args[1])

		if err != nil {
			return false, err
		}
		reader := bufio.NewReader(file)
		csvReader := csv.NewReader(reader)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client, err := mg.Client.StartListeningFromCsv(ctx)
		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}

		for {
			value, err := csvReader.Read()
			if err != nil {
				break
			}

			err = client.Send(&pb.RunConfiguration{SlaveID: value[0], ConfigName: value[1]})
			if err != nil {
				st := status.Convert(err)
				return false, errors.New(st.Message())
			}

		}
		job, err := client.CloseAndRecv()
		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}
		color.Green("Job successfully started. Job ID: " + strconv.Itoa(int(job.Id)))
		return true, nil
	case "-slave":
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if len(args) < 3 {
			return false, errors.New("insufficient args")
		}

		flag := strings.Contains(args[1], "@")
		if !flag {
			return false, errors.New("invalid slave identifier, must be user@host")
		}
		job, err := mg.Client.StartListeningSlave(ctx, &pb.RunConfiguration{SlaveID: args[1], ConfigName: args[2]})

		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}
		color.Green("Job ID: " + strconv.Itoa(int(job.Id)))
		return true, nil
	default:
		fmt.Println("Args not defined.")
	}

	return true, nil
}

func (mg *RPC) ConfigCommand(args []string) (bool, error) {
	if len(args) == 0 {
		return false, errors.New("invalid number of args")
	}
	switch args[0] {
	case "-file":
		//
		if len(args) < 3 {
			return false, errors.New("please supply path and name for the configuration file")
		}

		file, err := ioutil.ReadFile(args[1])
		if err != nil {
			return false, err
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := mg.Client.TransferConfigFile(ctx)
		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}

		var fileName string
		for i := 0; i < len(file); i += chunkSize {
			index := i + chunkSize
			if index > len(file) {
				index = len(file)
				fileName = args[2]
			}

			err := stream.Send(&pb.File{Data: file[i:index], Name: fileName})
			if err != nil {
				panic(err)
			}
		}

		_, err = stream.CloseAndRecv()
		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}

		fmt.Println("File transferred Successfully.")
		return true, nil
	case "-view":
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client, err := mg.Client.ReadConfigFile(ctx, &pb.Empty{})
		if err != nil {
			return false, err
		}

		var configFile []byte
		for {
			chnk, err := client.Recv()
			if err == io.EOF {
				break
			}
			configFile = append(configFile, chnk.Data...)
		}
		err = client.CloseSend()
		if err != nil {
			panic(err)
		}

		fmt.Println(string(configFile))

		return true, nil
	case "-list":
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client, err := mg.Client.ListConfigurations(ctx, &pb.Empty{})
		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}

		for {
			message, err := client.Recv()
			if err == io.EOF {
				break
			}
			fmt.Println(message.Text)
		}

		err = client.CloseSend()
		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}

		return true, nil
	case "-rm":
		if len(args) < 2 {
			return false, errors.New("invalid number of args")
		}
		configName := pb.Message{Text: args[1]}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err := mg.Client.RemoveConfigFile(ctx, &configName)
		if err != nil {
			st := status.Convert(err)
			return false, errors.New(st.Message())
		}
		return true, err
	default:
		return true, nil
	}

}

func (mg *RPC) SlaveCommand(args []string) (bool, error) {
	if len(args) == 0 {
		return false, errors.New("invalid number of args")
	}
	switch args[0] {
	case "-new":
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
	case "-rm":
		// Delete specific slave identified by Username@Host
		if len(args) < 2 {
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
	case "-list":
		// List Slaves.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		client, err := mg.Client.ListSlaves(ctx, &pb.Empty{})
		if err != nil {
			return false, nil
		}
		color.Cyan("List of slave machines: ")
		color.Cyan("Host\tUsername\tPassword")
		var count = 0
		color.Set(color.FgHiGreen)
		for {
			slave, err := client.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return false, err
			}
			count += 1
			fmt.Println(slave.Host + "\t" + slave.Username + "\t" + slave.Password)
		}
		color.Unset()
		err = client.CloseSend()
		if err != nil {
			return false, err
		}
		color.Cyan("Total Machines: " + strconv.Itoa(count))
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
