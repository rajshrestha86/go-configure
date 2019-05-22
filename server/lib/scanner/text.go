package scanner

import (
	"encoding/json"
	"io"
	"log"
	"rara/server/lib/db"
	"rara/server/lib/parser"
)

// Parses output for text processes.
func ParseTextOutput(exit chan bool, config map[string]interface{}, stdout *io.Reader, slave map[string]string, db *db.DB) {
	var err error
	var commandMeta = make(map[string]interface{})
	var outputChannel = make(chan []byte)
	var identifier = slave["username"]+"@"+slave["host"]
	var timestamp = slave["timestamp"]
	var configName = slave["configName"]

	// Get a TextParser
	parser := parser.InitializeTextOutput()

	commandMeta["name"] = config["name"]
	commandMeta["output"] = config["output"]
	commandMeta["rules"] = config["rules"]
	commandMeta["raw_output"] = []string{}
	commandMeta["matched_output"] = []string{}
	commandMetaBytes, _ := json.Marshal(commandMeta)

	DbKey := identifier+":"+configName+":"+commandMeta["name"].(string)+":"+timestamp

	err = db.Write(DbKey, commandMetaBytes)
	if err != nil {
		log.Fatal("Error writing to data.")
		panic(err)
	}

	go scanOutput(outputChannel, *stdout)

	var deserializedCmdMeta map[string]interface{}
	var serializedCmdMeta []byte
	for line := range outputChannel {
		cmdMeta, err := db.Read(DbKey)
		if err != nil {
			log.Fatal("Error reading data.")
			panic(err)
		}
		parser.Populate(line)
		_ = json.Unmarshal([]byte(cmdMeta), &deserializedCmdMeta)
		matched := parser.Evaluate_regex(commandMeta["rules"].([]interface{}))
		if len(matched) > 0 {
			deserializedCmdMeta["matched_output"] = append(deserializedCmdMeta["matched_output"].([]interface{}), matched...)
		}
		deserializedCmdMeta["raw_output"] = append(deserializedCmdMeta["raw_output"].([]interface{}), parser.To_string())
		serializedCmdMeta, _ = json.Marshal(deserializedCmdMeta)

		err = db.Write(DbKey, serializedCmdMeta)
		if err != nil {
			log.Fatal("Error writing to data.")
			panic(err)
		}

	}
	exit <- true

}