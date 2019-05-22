package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"io/ioutil"
	"log"
	"os"
	pb "rara/client/grpc/proto"
	connManager "rara/server/lib/connector"
	"rara/server/lib/db"
	"rara/server/lib/scanner"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	chunkSize         = 100
	configurationsDir = "/home/pi/go/src/rara/server/configurations"
)

type Server struct {
	Db           *db.DB
	SlaveJobs    map[int32][]string  // Map of JobIDs and the Machines which uniquely identifies slave.
	Jobs         map[int32]chan bool // Channels for Each Job (Can be used to stop the job.)
	Running      map[string]int32    // List of all running configurations.
	JobIDCounter int32
}

func (s *Server) DeleteAllOutputOfHost(ctxt context.Context, slave *pb.Slave) (*pb.Status, error) {

	var outputMap map[string]interface{}
	result, err := s.Db.Read(slave.Id + ":" + "outputs")
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return &pb.Status{Status: false}, errors.New("no outputs for the host")
		}
		return &pb.Status{Status: false}, err
	}

	_ = json.Unmarshal(result, &outputMap)
	for k := range outputMap {
		err = s.DeleteOutputWithTimestamp(slave.Id, k)
		if err != nil {
			return &pb.Status{Status: false}, err
		}
	}

	return &pb.Status{Status: true}, nil
}

func (s *Server) GetOutput(id string) (map[string]interface{}, error) {
	outputBytes, err := s.Db.Read(id)
	var outputMap = make(map[string]interface{})
	if err == badger.ErrKeyNotFound {
		return nil, errors.New("no output for given id")
	}
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(outputBytes, &outputMap)
	if err != nil {
		return nil, err
	}
	return outputMap, err
}

func (s *Server) GetMatchedOutput(output *pb.OutputId, srv pb.CLI_GetMatchedOutputServer) error {
	outputMap, err := s.GetOutput(output.Id)
	if err != nil {
		return err
	}
	for _, each := range outputMap["matched_output"].([]interface{}) {
		err = srv.Send(&pb.Message{Text: each.(string)})
		if err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

func (s *Server) DeleteOutputWithTimestamp(slaveId, timestamp string) error {

	var txn = func(txn *badger.Txn) error {
		var deserialized = make(map[string]interface{})
		item, err := txn.Get([]byte(slaveId + ":" + "outputs"))
		s.Db.WriteLock()
		defer s.Db.WriteUnlock()

		if err == badger.ErrKeyNotFound {
			txn.Discard()
			return errors.New("output not found")
		}

		cmdMeta, err := item.ValueCopy(nil)
		_ = json.Unmarshal(cmdMeta, &deserialized)

		if deserialized[timestamp] == nil {
			txn.Discard()
			return errors.New("output not found")
		}
		commands := deserialized[timestamp].(map[string]interface{})["commands"]
		configName := deserialized[timestamp].(map[string]interface{})["configName"]
		for _, value := range commands.([]interface{}) {
			key := []byte(slaveId + ":" + configName.(string) + ":" + value.(string) + ":" + timestamp)
			err := txn.Delete(key)
			if err != nil {
				txn.Discard()
				return err
			}
		}

		delete(deserialized, timestamp)
		serialized, _ := json.Marshal(deserialized)
		err = txn.Set([]byte(slaveId+":outputs"), serialized)
		if err != nil {
			txn.Discard()
			return errors.New("error delete operation unsuccessful ")
		}
		return err
	}
	err := s.Db.StartTransaction(txn)
	return err
}

func (s *Server) DeleteOutputWithGivenTimestamp(ctx context.Context, output *pb.OutputId) (*pb.Status, error) {
	arr := strings.Split(output.Id, ":")
	if len(arr) != 2 {
		return &pb.Status{Status: false}, errors.New("invalid identifier")
	}
	err := s.DeleteOutputWithTimestamp(arr[0], arr[1])
	if err != nil {
		return &pb.Status{Status: false}, err
	}
	return &pb.Status{Status: true}, nil
}

func (s *Server) DeleteOutput(ctxt context.Context, output *pb.OutputId) (*pb.Status, error) {

	arr := strings.Split(output.Id, ":")
	if len(arr) != 4 {
		return &pb.Status{Status: false}, errors.New("invalid identifier")
	}
	var txn = func(txn *badger.Txn) error {
		var deserialized = make(map[string]interface{})
		item, err := txn.Get([]byte(arr[0] + ":" + "outputs"))
		s.Db.WriteLock()
		defer s.Db.WriteUnlock()

		if err == badger.ErrKeyNotFound {
			fmt.Println("Discard transaction.")
			txn.Discard()
			return errors.New("output not found")
		}

		cmdMeta, err := item.ValueCopy(nil)
		_ = json.Unmarshal(cmdMeta, &deserialized)

		timestamp := arr[len(arr)-1]
		command := arr[len(arr)-2]
		if deserialized[timestamp] == nil {
			txn.Discard()
			return errors.New("output not found")
		}
		commands := deserialized[timestamp].(map[string]interface{})["commands"]
		var commandFound = false
		var newCommands []string
		for _, value := range commands.([]interface{}) {
			if value == command {
				commandFound = true
			} else {
				newCommands = append(newCommands, value.(string))
			}
		}

		if !commandFound {
			txn.Discard()
			return errors.New("output not found")
		}

		if newCommands == nil {
			delete(deserialized, timestamp)
		} else {
			deserialized[timestamp].(map[string]interface{})["commands"] = newCommands
		}

		serialized, _ := json.Marshal(deserialized)
		err = txn.Set([]byte(arr[0]+":outputs"), serialized)
		if err != nil {
			txn.Discard()
			return errors.New("error delete operation unsuccessful ")
		}
		err = txn.Delete([]byte(output.Id))
		if err != nil {
			txn.Discard()
			return err
		}
		fmt.Println("Trying to commit.")
		//err = txn.Commit()
		return err

	}

	err := s.Db.StartTransaction(txn)
	if err != nil {
		log.Println(err)
		return &pb.Status{Status: false}, err
	}
	return &pb.Status{Status: true}, nil

}

func (s *Server) ListOutputKeysForHost(slave *pb.Slave, srv pb.CLI_ListOutputKeysForHostServer) error {
	slaveId := slave.Id
	outputBytes, err := s.Db.Read(slaveId + ":outputs")
	var outputMap = make(map[string]interface{})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return errors.New("no outputs for given slave")
		}
		return err
	}
	err = json.Unmarshal(outputBytes, &outputMap)
	if err != nil {
		panic(err)
	}

	var keys []int
	for k := range outputMap {
		integer, _ := strconv.Atoi(k)
		keys = append(keys, integer)
	}
	sort.Ints(keys)

	for _, k := range keys {
		str := strconv.Itoa(k)
		v := outputMap[str]
		value := v.(map[string]interface{})
		for _, each := range value["commands"].([]interface{}) {
			outputId := slaveId + ":" + value["configName"].(string) + ":" + each.(string) + ":" + str
			err := srv.Send(&pb.Message{Text: outputId})
			if err != nil {
				panic(err)
			}
		}

	}
	return nil
}

func (s *Server) GetRawOutput(output *pb.OutputId, srv pb.CLI_GetRawOutputServer) error {
	outputMap, err := s.GetOutput(output.Id)
	if err != nil {
		return err
	}

	for _, each := range outputMap["raw_output"].([]interface{}) {
		err = srv.Send(&pb.Message{Text: each.(string)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (s *Server) StopJob(jobId int32, controlChannel chan bool) {
	close(controlChannel)
	delete(s.Jobs, jobId)
	slaves := s.SlaveJobs[jobId]
	for _, each := range slaves {
		delete(s.Running, each)
	}
	delete(s.SlaveJobs, jobId)
}

func (s *Server) DeleteJob(runId string) {
	jobId := s.Running[runId]
	slaves := s.SlaveJobs[jobId]
	if len(slaves) == 1 {
		delete(s.SlaveJobs, jobId)
		delete(s.Jobs, jobId)
	} else {
		var remainingSlaves []string
		for _, each := range slaves {
			if each == runId {
				continue
			}
			remainingSlaves = append(remainingSlaves, each)
		}
		s.SlaveJobs[jobId] = remainingSlaves
	}
	delete(s.Running, runId)
}

func (s *Server) StopAllRunningJobs(ctxt context.Context, _ *pb.Empty) (*pb.Status, error) {
	for k, v := range s.Jobs {
		go s.StopJob(k, v)
	}

	s.JobIDCounter = 0
	return &pb.Status{Status: true}, nil
}

func (s *Server) RemoveConfigFile(ctxt context.Context, msg *pb.Message) (*pb.Status, error) {
	configName := msg.Text
	if _, err := os.Stat(configurationsDir + "/" + configName + ".json"); os.IsNotExist(err) {
		return &pb.Status{Status: false}, errors.New("no config file with given name")
	} else {
		err = os.Remove(configurationsDir + "/" + configName + ".json")
		if err != nil {
			return &pb.Status{Status: false}, errors.New("cannot remove file error: " + err.Error())
		}
		return &pb.Status{Status: true}, nil
	}
}

func (s *Server) ListConfigurations(_ *pb.Empty, srv pb.CLI_ListConfigurationsServer) error {
	var err error
	files, err := ioutil.ReadDir(configurationsDir)
	if err != nil {
		log.Fatal(err)
		return err
	}

	for _, f := range files {
		err = srv.Send(&pb.Message{Text: f.Name()})
		if err != nil {
			log.Fatal(err)
			return err
		}
	}

	err = srv.Send(&pb.Message{Text: "Total config files: " + strconv.Itoa(len(files))})
	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

func (s *Server) ViewAllRunningJobs(_ *pb.Empty, srv pb.CLI_ViewAllRunningJobsServer) error {
	var err error
	if len(s.Jobs) == 0 {
		err = srv.Send(&pb.Message{Text: "No jobs running currently."})
		if err != nil {
			log.Fatal(err)
			return err
		}
	}

	for k := range s.Jobs {
		slaves := s.SlaveJobs[k]
		for _, slave := range slaves {
			err = srv.Send(&pb.Message{Text: "JobId: " + strconv.Itoa(int(k)) + "\t Slave: " + slave})
			if err != nil {
				log.Fatal(err)
				return err
			}
		}

	}

	return nil
}

func (s *Server) GenerateJobID() (int32, chan bool) {
	s.JobIDCounter += 1
	jobId := s.JobIDCounter
	channel := make(chan bool)
	s.Jobs[jobId] = channel
	return jobId, channel
}

func (s *Server) SetJobID(identifier string, jobId int32) {
	slaves, ok := s.SlaveJobs[jobId]
	s.Running[identifier] = jobId
	if ok {
		s.SlaveJobs[jobId] = append(slaves, identifier)
	} else {
		s.SlaveJobs[jobId] = []string{identifier}
	}
}

func (s *Server) StartListeningFromCsv(srv pb.CLI_StartListeningFromCsvServer) error {
	var err error
	var requestedSlaves []string
	var RunConfigurations []pb.RunConfiguration

	// Stream list of Machines from the CSV.
	for {
		config, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(config.SlaveID, config.ConfigName, err)
		}
		RunConfigurations = append(RunConfigurations, *config)
	}

	/* 	In order to control errors before executing jobs we introduce a startSignal channel.
	Job will only start after getting a startSignal. If any error occurs then we don't send startSignal. Instead
	we will kill all the jobs. It is useful if we encounter a running job on the list.*/
	startSignal := make(chan bool)

	/* 	After successfully receiving machines create a common jobId and controlChannel.
	Later the job can be stopped using this control channel. */
	jobId, controlChannel := s.GenerateJobID()

	for _, each := range RunConfigurations {
		requestedSlaves = append(requestedSlaves, each.SlaveID)
		res, err := s.Db.Read("slave:" + each.SlaveID)

		// It means the slave hasn't been setup. We can log it or return error.
		// Right now we assume to send an error to client.

		if err != nil {
			if err == badger.ErrKeyNotFound {
				log.Println("Slave not found: ", each.SlaveID)
				close(startSignal)
				s.StopJob(jobId, controlChannel)
				return status.Error(codes.NotFound, "slave not found on the server: "+each.SlaveID)
			}
			return err
		}

		// Check if config File exists.
		if _, err := os.Stat(configurationsDir + "/" + each.ConfigName + ".json"); os.IsNotExist(err) {
			log.Println("Config file not found: ", each.ConfigName)
			return errors.New("no config file with given name on the server for host: "+each.SlaveID+" : "+each.ConfigName)
		}

		// Check if similar job is running.
		jobId, ok := s.Running[each.SlaveID+":"+each.ConfigName]
		if ok {
			log.Println("There is already a running job for the slave and config.")
			close(startSignal)
			s.StopJob(jobId, controlChannel)
			return errors.New("job is already running: " + each.SlaveID + ":" + each.ConfigName + ". please stop it first. JobId: "+strconv.Itoa(int(jobId)))
		}

		// This slave has the credentials necessary for the sshConnection.
		var deserializedSlave map[string]string
		_ = json.Unmarshal(res, &deserializedSlave)

		connection := connManager.NewConnection(deserializedSlave["username"], deserializedSlave["host"], deserializedSlave["password"])
		err = connection.ConnectRemote()
		if err != nil {
			log.Println(err)
			close(startSignal)
			s.StopJob(jobId, controlChannel)
			return errors.New("cannot establish remote connection to : " + each.SlaveID)
		}

		// We save the jobId information for this slave in our list and initiate job.
		s.SetJobID(each.SlaveID+":"+each.ConfigName, jobId)
		go s.ListenServer(connection, each.ConfigName, deserializedSlave, controlChannel, startSignal)

	}

	// If Everything Alright send jobID.
	err = srv.SendAndClose(&pb.Job{Id: int32(jobId)})
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(RunConfigurations); i++ {
		startSignal <- true
	}
	return nil
}

func (s *Server) StartListeningSlave(ctxt context.Context, config *pb.RunConfiguration) (*pb.Job, error) {

	res, err := s.Db.Read("slave:" + config.SlaveID)

	// It means the slave hasn't been setup. We can log it or return error.
	// Right now we assume to send an error to client.

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return &pb.Job{Id: 0}, status.Error(codes.NotFound, "slave not found on the server: "+config.SlaveID)
		}
		log.Println(err)
		return &pb.Job{Id: -1}, err
	}

	_, ok := s.Running[config.SlaveID+":"+config.ConfigName]
	if ok {
		return &pb.Job{Id: -1}, errors.New("job is already running: " + config.SlaveID + ":" + config.ConfigName + ". please stop it first.")
	}

	if _, err := os.Stat(configurationsDir + "/" + config.ConfigName + ".json"); os.IsNotExist(err) {
		return &pb.Job{Id: -1}, errors.New("no config file with given name")
	}

	// This slave has the credentials necessary for the sshConnection.
	var deserializedSlave map[string]string
	_ = json.Unmarshal(res, &deserializedSlave)

	connection := connManager.NewConnection(deserializedSlave["username"], deserializedSlave["host"], deserializedSlave["password"])
	err = connection.ConnectRemote()
	if err != nil {
		return &pb.Job{Id: 100}, errors.New("cannot connect to slave: " + err.Error())
	}

	// Start Job with a StartSignal and signal it to Run
	startSignal := make(chan bool)
	jobId, controlChannel := s.GenerateJobID()
	s.SetJobID(config.SlaveID+":"+config.ConfigName, jobId)
	go s.ListenServer(connection, config.ConfigName, deserializedSlave, controlChannel, startSignal)
	startSignal <- true
	return &pb.Job{Id: jobId}, nil

}

func (s *Server) StopListeningJob(ctxt context.Context, job *pb.Job) (*pb.Status, error) {
	controlChannel, ok := s.Jobs[job.Id]
	if !ok {
		return &pb.Status{Status: false}, errors.New("job isn't running")
	}
	s.StopJob(job.Id, controlChannel)
	return &pb.Status{Status: true}, nil
}

func (s *Server) ReadConfigFile(_ *pb.Empty, stream pb.CLI_ReadConfigFileServer) error {
	file, err := ioutil.ReadFile("./config.json")
	if err != nil {
		return err
	}

	for i := 0; i < len(file); i += chunkSize {
		index := i + chunkSize
		if index > len(file) {
			index = len(file)
		}

		err := stream.Send(&pb.File{Data: file[i:index], Name: ""})
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (s *Server) ListenServer(connection connManager.SSHCONNECTION, configName string, slave map[string]string, controlChannel chan bool, startSignal chan bool) {
	// Wait for start signal. We can only continue after getting a start signal.
	timestamp := time.Now().Format("20060102150405")
	slave["timestamp"] = timestamp
	slave["configName"] = configName
	_, ok := <-startSignal
	if !ok {
		connection.Close()
		return
	}

	configFile, err := ioutil.ReadFile(configurationsDir + "/" + configName + ".json")
	if err != nil {
		log.Println(err)
		return
	}
	var configurations []map[string]interface{}
	_ = json.Unmarshal(configFile, &configurations)

	/*
		 AddMeta to DB
		 Meta in the form of:
		 Key: user@localhost:outputs
		 Value: [
		 			{
		 			"timestamp": {
		 				"configName": ConfigName,
		 				"log_output": LogHead,
		 				"Commands":[List of commands]...
								}
					}...]
	*/
	var deserialized = make(map[string]interface{})
	var commands []string
	slaveId := slave["username"] + "@" + slave["host"]

	for _, config := range configurations {
		commands = append(commands, config["name"].(string))
	}

	innerMap := make(map[string]interface{})
	innerMap["configName"] = configName
	innerMap["log_output"] = "LogID"
	innerMap["commands"] = commands

	// Lock Read to avoid race conditions.
	var txn = func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(slaveId + ":" + "outputs"))
		if err != badger.ErrKeyNotFound {
			cmdMeta, _ := item.ValueCopy(nil)
			_ = json.Unmarshal(cmdMeta, &deserialized)
		}
		s.Db.WriteLock()
		deserialized[slave["timestamp"]] = innerMap
		serialized, _ := json.Marshal(deserialized)
		err = txn.Set([]byte(slaveId+":outputs"), serialized)

		s.Db.WriteUnlock()
		return err
	}

	err = s.Db.StartTransaction(txn)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	// Wait Groups are used to keep the connection alive until command execution completes.
	numbCmds := len(configurations)
	var wg sync.WaitGroup
	wg.Add(numbCmds)

	// Each Commands are run concurrently independent of each other as a go routine..
	for _, config := range configurations {
		if config["output"] == "table" {
			go func(config map[string]interface{}) {
				identifier := slaveId + ":" + configName + ":" + config["name"].(string) + ":" + timestamp
				log.Println("Started listening: ", identifier)
				defer wg.Done()
				var completionStatus = make(chan bool)
				sess, err := connection.Client.NewSession()
				stdout, err := sess.StdoutPipe()

				// Go routine to parse output separately because Session.run command is blocking.
				// Also pass timestamp and configuration name when the listening was started and save its corresponding output.
				go scanner.ParseTableOutput(completionStatus, config, &stdout, slave, s.Db)
				go func() {
					err = sess.Run(config["command"].(string))
					// Later this error needs to be logged.
					if err != nil {
						log.Println(err)
					}
				}()

				// Check commands from controlChannel or CompletionStatus of command..
				var exitLoop = false
				for {
					select {
					case <-controlChannel:
						_ = sess.Close()
						exitLoop = true
						fmt.Println("True controlChannel")
						log.Println("Got Kill Signal: ", identifier)
					case <-completionStatus:
						_ = sess.Close()
						exitLoop = true
						log.Println("Completed listening server: ", identifier)
					default:
						time.Sleep(time.Second * 5)
					}
					if exitLoop {
						break
					}
				}
			}(config)
		} else if config["output"] == "text" {
			go func(config map[string]interface{}) {
				identifier := slaveId + ":" + configName + ":" + config["name"].(string) + ":" + timestamp
				log.Println("Started listening: ", identifier)
				defer wg.Done()
				var completionStatus = make(chan bool)
				sess, _ := connection.Client.NewSession()
				stdout, _ := sess.StdoutPipe()
				go scanner.ParseTextOutput(completionStatus, config, &stdout, slave, s.Db)
				go func() {
					err = sess.Run(config["command"].(string))

					// Later this error needs to be logged.
					if err != nil {
						log.Println(err)
					}
				}()

				var exitLoop = false
				for {
					select {
					case _, ok := <-controlChannel:
						if !ok {
							_ = sess.Close()
							exitLoop = true
							log.Println("Got Kill Signal: ", identifier)
						}
					case <-completionStatus:
						_ = sess.Close()
						exitLoop = true
						log.Println("Completed listening server: ", identifier)
					default:
						time.Sleep(time.Second * 5)
					}
					if exitLoop {
						break
					}
				}
			}(config)
		}
	}

	wg.Wait()
	connection.Close()

	s.DeleteJob(slaveId + ":" + slave["configName"])

}

func (s *Server) TransferConfigFile(stream pb.CLI_TransferConfigFileServer) error {
	var file []byte
	var fileName string
	for {
		chnk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		file = append(file, chnk.Data...)
		fileName = chnk.Name
	}

	err := ioutil.WriteFile(configurationsDir+"/"+fileName+".json", file, 0666)
	if err != nil {
		log.Println("Error saving configurations file.")
	}

	_ = stream.SendAndClose(&pb.Status{Status: true})
	return nil
}

func (s *Server) AddSlave(ctx context.Context, slv *pb.Slave) (*pb.Status, error) {
	var slave = make(map[string]string)
	var slaves = make(map[string]interface{})
	if slv.Host == "" || slv.Username == "" || slv.Password == "" {
		return &pb.Status{Status: false}, errors.New("error: nil host@username or nil password")
	}
	slave["username"] = slv.Username
	slave["password"] = slv.Password
	slave["host"] = slv.Host
	userHost := slave["username"] + "@" + slave["host"]
	res, err := s.Db.Read("slaves")
	if err == badger.ErrKeyNotFound {
		var value interface{}
		value = 1
		slaves["count"] = value.(int)
		slaves["machines"] = []string{userHost}
	} else {
		_ = json.Unmarshal(res, &slaves)
		slaves["count"] = int(slaves["count"].(float64)) + 1
		slaves["machines"] = append(slaves["machines"].([]interface{}), userHost)
	}
	serialized, _ := json.Marshal(slaves)

	// Initiate as a single transaction.
	var txn = func(txn *badger.Txn) error {
		err = s.Db.Write("slaves", serialized)
		if err != nil {
			txn.Discard()
			return err
		}

		serializedSlave, _ := json.Marshal(slave)
		err = s.Db.Write("slave:"+userHost, serializedSlave)
		if err != nil {
			txn.Discard()
			return err
		}

		return err
	}

	err = s.Db.StartTransaction(txn)
	if err != nil {
		log.Println("Add Slave: ", err)
		return &pb.Status{Status: false}, err
	}
	return &pb.Status{Status: true}, nil
}

func (s *Server) DeleteSlave(ctxt context.Context, slv *pb.Slave) (*pb.Status, error) {
	var slaves = make(map[string]interface{})
	var transaction = func(txn *badger.Txn) error {
		res, err := s.Db.Read("slaves")
		if err == badger.ErrKeyNotFound {
			return errors.New("no slaves on the machine")
		}

		_ = json.Unmarshal(res, &slaves)
		userHost := slv.Username + "@" + slv.Host
		_, err = s.Db.Read("slave:" + userHost)
		if err == badger.ErrKeyNotFound {
			txn.Discard()
			return errors.New("no slave with given identifier: " + userHost)
		}

		var pos = -1
		for index, slave := range slaves["machines"].([]interface{}) {
			if slave == userHost {
				pos = index
				slaves["machines"] = append(slaves["machines"].([]interface{})[:index], slaves["machines"].([]interface{})[index+1:]...)
				break
			}
		}

		if pos < 0 {
			return errors.New("inconsistent information on db for host: " + userHost)
		}

		serialized, _ := json.Marshal(slaves)
		err = s.Db.Write("slaves", serialized)
		if err != nil {
			txn.Discard()
			return err
		}

		err = txn.Delete([]byte("slave:" + userHost))
		if err != nil {
			txn.Discard()
			log.Println(err)
			return errors.New("error delete operation unsuccessful ")
		}

		return nil
	}

	err := s.Db.StartTransaction(transaction)
	if err != nil {
		return &pb.Status{Status: false}, err
	}

	return &pb.Status{Status: true}, err
}

func (s *Server) ListSlaves(_ *pb.Empty, stream pb.CLI_ListSlavesServer) error {
	var slaves = make(map[string]interface{})
	var deserializeSlave map[string]string
	res, err := s.Db.Read("slaves")
	if err == badger.ErrKeyNotFound {
		return nil
	}
	_ = json.Unmarshal(res, &slaves)
	for _, each := range slaves["machines"].([]interface{}) {
		res, err := s.Db.Read("slave:" + each.(string))
		if err != nil {
			return err
		}

		_ = json.Unmarshal(res, &deserializeSlave)
		err = stream.Send(&pb.Slave{Username: deserializeSlave["username"], Host: deserializeSlave["host"], Password: deserializeSlave["password"]})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) DeleteSlaves(context.Context, *pb.Empty) (*pb.Status, error) {
	var slaves = make(map[string]interface{})
	var transaction = func(txn *badger.Txn) error {
		res, err := s.Db.Read("slaves")
		if err == badger.ErrKeyNotFound {
			return errors.New("no slaves on the machine")
		}

		_ = json.Unmarshal(res, &slaves)

		for _, slave := range slaves["machines"].([]interface{}) {
			err = txn.Delete([]byte("slave:" + slave.(string)))

			if err != nil {
				txn.Discard()
				log.Println(err)
				return errors.New("error delete operation unsuccessful ")
			}
		}

		err = txn.Delete([]byte("slaves"))
		if err != nil {
			txn.Discard()
			log.Println(err)
			return errors.New("error delete operation unsuccessful ")
		}

		return nil
	}

	err := s.Db.StartTransaction(transaction)
	if err != nil {
		return &pb.Status{Status: false}, err
	}

	return &pb.Status{Status: true}, err
}
