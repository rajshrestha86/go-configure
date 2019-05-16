package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"io"
	"io/ioutil"
	"rara/client"
	connManager "rara/server/lib/connector"
	"rara/server/lib/scanner"
	"sync"
)

type Server struct {
	Db *DB
}

func (s *Server) ListenServer(slave map[string]string) {
	connection := connManager.NewConnection(slave["username"], slave["host"], slave["password"])
	connection.ConnectRemote()
	defer connection.Close()

	configFile, err := ioutil.ReadFile("./config.json")
	if err != nil {
		fmt.Println(err)
	}
	var configurations []map[string]interface{}
	_ = json.Unmarshal(configFile, &configurations)

	numbCmds := len(configurations)
	var wg sync.WaitGroup
	wg.Add(numbCmds)

	// Each Commands are run concurrently independent of each other.
	for _, config := range configurations {
		if config["output"] == "table" {
			go func(config map[string]interface{}) {
				defer wg.Done()
				var exitFlag = make(chan bool)
				sess, _ := connection.Client.NewSession()
				stdout, _ := sess.StdoutPipe()
				go scanner.ParseTableOutput(exitFlag, config, &stdout, slave, s.Db)
				_ = sess.Run(config["command"].(string))
				<-exitFlag
				_ = sess.Close()
			}(config)
		} else if config["output"] == "text" {
			go func(config map[string]interface{}) {
				defer wg.Done()
				var exitFlag = make(chan bool)
				sess, _ := connection.Client.NewSession()
				stdout, _ := sess.StdoutPipe()
				go scanner.ParseTextOutput(exitFlag, config, &stdout, slave, s.Db)
				_ = sess.Run(config["command"].(string))
				<-exitFlag
				_ = sess.Close()
			}(config)
		}
	}

	// Wait Group to wait for the execution of all the commands.
	wg.Wait()
}

func (s *Server) StartListeningServers(context.Context, *main.Empty) (*main.Status, error) {
	var slaves = make(map[string]interface{})
	var deserializedSlave map[string]string
	res, err := s.Db.Read("slaves")
	if err == badger.ErrKeyNotFound {
		return &main.Status{Status: false}, errors.New("no slaves configured")
	}
	_ = json.Unmarshal(res, &slaves)
	for _, each := range slaves["machines"].([]interface{}) {
		res, err := s.Db.Read("slave:" + each.(string))
		if err != nil {
			return &main.Status{Status: false}, err
		}

		_ = json.Unmarshal(res, &deserializedSlave)
		go s.ListenServer(deserializedSlave)
	}
	return &main.Status{Status: true}, nil
}

func (s *Server) StopListeningServers(context.Context, *main.Empty) (*main.Status, error) {
	panic("implement me")
}

func (s *Server) StopListening(context.Context, *main.Slave) (*main.Status, error) {
	panic("implement me")
}

func (s *Server) TransferConfigFile(stream main.CLI_TransferConfigFileServer) error {
	var file []byte
	for {
		chnk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		file = append(file, chnk.Data...)
	}

	err := ioutil.WriteFile("./config_server.json", file, 0666)
	if err != nil {
		fmt.Println("Error saving file.")
	}
	_ = stream.SendAndClose(&main.Status{Status: true})
	return nil
}

func (s *Server) AddSlave(ctx context.Context, slv *main.Slave) (*main.Status, error) {
	var slave = make(map[string]string)
	var slaves = make(map[string]interface{})
	if slv.Host == "" || slv.Username == "" || slv.Password == "" {
		return &main.Status{Status: false}, errors.New("error: nil host@username or nil password")
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
		return &main.Status{Status: false}, err
	}
	fmt.Println("Invoked AddSlave method.")
	fmt.Println(slv.Username, slv.Password, slv.Host)
	return &main.Status{Status: true}, nil
}

func (s *Server) DeleteSlave(ctxt context.Context, slv *main.Slave) (*main.Status, error) {
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
			fmt.Println(err)
			return errors.New("error delete operation unsuccessful ")
		}

		return nil
	}

	err := s.Db.StartTransaction(transaction)
	if err != nil {
		return &main.Status{Status: false}, err
	}

	return &main.Status{Status: true}, err
}

func (s *Server) ListSlaves(_ *main.Empty, stream main.CLI_ListSlavesServer) error {
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
		err = stream.Send(&main.Slave{Username: deserializeSlave["username"], Host: deserializeSlave["host"], Password: deserializeSlave["password"]})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) DeleteSlaves(context.Context, *main.Empty) (*main.Status, error) {
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
				fmt.Println(err)
				return errors.New("error delete operation unsuccessful ")
			}
		}

		err = txn.Delete([]byte("slaves"))
		if err != nil {
			txn.Discard()
			fmt.Println(err)
			return errors.New("error delete operation unsuccessful ")
		}

		return nil
	}

	err := s.Db.StartTransaction(transaction)
	if err != nil {
		return &main.Status{Status: false}, err
	}

	return &main.Status{Status: true}, err
}
