package scanner

import (
	"encoding/json"
	"io"
	"log"
	"rara/server/lib/db"
	"rara/server/lib/parser"
)

func ParseTableOutput(completeFlag chan bool, config map[string]interface{}, stdout *io.Reader, slave map[string]string, db *db.DB) {
	var err error
	var commandInfo = make(map[string]interface{})
	var identifier = slave["username"]+"@"+slave["host"]
	var timestamp = slave["timestamp"]
	var configName = slave["configName"]
	commandInfo["name"] = config["name"].(string)
	tableConfig := config["table_restruct"]

	// Output Parser Based on the table_format.
	outConfig := tableConfig.([]interface{})
	rowParser := parser.InitializeTableRow(outConfig)

	// Some table may have unstructured row. We should ignore those.
	ignore := config["ignore"]
	var checkIgnored = func(s []interface{}, e int) bool {
		for _, a := range s {
			if a == float64(e) {
				return true
			}
		}
		return false
	}

	var scanPosition  = 0
	var outputChannel = make(chan []byte)
	var deserializedCmdMeta map[string]interface{}

	// Scanning Output is a blocking Job, instead we run it on separate goroutine and communicate
	// using channel.
	go scanOutput(outputChannel, *stdout)

	commandInfo["outputType"] = config["output"]
	commandInfo["rules"] = config["rules"]
	commandInfo["raw_output"] = []string{}
	commandInfo["matched_output"] = []string{}
	commandInfo["error_log"] = []string{}
	commandInfoBytes, _ := json.Marshal(commandInfo)

	DbKey := identifier+":"+configName+":"+commandInfo["name"].(string)+":"+timestamp
	err = db.Write(DbKey, commandInfoBytes)
	if err != nil {
		log.Println("Error writing to data: ", err)
	}

	for row := range outputChannel {
		scanPosition += 1
		if ignore !=nil{
			if checkIgnored(ignore.([]interface{}), scanPosition) {
				continue
			}
		}
		rawOutput := string(row)
		rowParser.Populate(&rawOutput)

		cmdMeta, err := db.Read(DbKey)
		if err != nil {
			log.Println("Error Reading Data: ", err)
		}
		_ = json.Unmarshal([]byte(cmdMeta), &deserializedCmdMeta)

		deserializedCmdMeta["raw_output"] = append(deserializedCmdMeta["raw_output"].([]interface{}), rawOutput)
		ruleMatched, err := rowParser.EvaluateExpressions(commandInfo["rules"].(string))
		if err != nil {
			deserializedCmdMeta["error_log"] = append(deserializedCmdMeta["error_log"].([]interface{}), err.Error())
		} else {
			if ruleMatched {
				deserializedCmdMeta["matched_output"] = append(deserializedCmdMeta["matched_output"].([]interface{}), string(rowParser.To_json()))
			}
		}
		commandInfoBytes, _ = json.Marshal(deserializedCmdMeta)
		err = db.Write(DbKey, commandInfoBytes)
		if err != nil {
			log.Println("Error writing to data: ", err)
		}
	}
	completeFlag <- true
}
