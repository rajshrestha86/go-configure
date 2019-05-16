package scanner

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"rara/server/lib"
)

// Parses output for text processes.
func ParseTextOutput(exit chan bool, config map[string]interface{}, stdout *io.Reader, slave map[string]string, db *lib.DB) {
	var err error
	var commandMeta = make(map[string]interface{})
	var combinedOutput []byte
	var outputChannel = make(chan []byte)
	var identifier = slave["username"]+"@"+slave["host"]

	commandMeta["name"] = config["name"]
	commandMeta["output"] = config["output"]
	commandMeta["rules"] = config["rules"]
	commandMeta["raw_output"] = []string{}

	commandMetaBytes, _ := json.Marshal(commandMeta)
	err = db.Write(identifier+"_"+commandMeta["name"].(string), commandMetaBytes)
	if err != nil {
		log.Fatal("Error writing to data.")
		panic(err)
	}

	go scanOutput(outputChannel, *stdout)

	var deserializedCmdMeta map[string]interface{}
	var serializedCmdMeta []byte
	for line := range outputChannel {
		combinedOutput = append(combinedOutput, line...)
		cmdMeta, err := db.Read(identifier + "_" + commandMeta["name"].(string))
		if err != nil {
			log.Fatal("Error reading data.")
			panic(err)
		}
		_ = json.Unmarshal([]byte(cmdMeta), &deserializedCmdMeta)
		deserializedCmdMeta["raw_output"] = combinedOutput
		serializedCmdMeta, _ = json.Marshal(deserializedCmdMeta)
		err = db.Write(identifier+"_"+commandMeta["name"].(string), serializedCmdMeta)
		if err != nil {
			log.Fatal("Error writing to data.")
			panic(err)
		}

		key := identifier + "_" + commandMeta["name"].(string) + "_output"
		err = db.Write(key, combinedOutput)
		if err != nil {
			fmt.Println("There is a error in DB write.")
			panic(err)
		}
	}

	exit <- true

}