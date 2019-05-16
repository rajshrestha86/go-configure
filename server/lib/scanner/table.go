package scanner

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"rara/server/lib"
	"rara/server/lib/parser"
	"strconv"
)

func ParseTableOutput(flag chan bool, config map[string]interface{}, stdout *io.Reader, slave map[string]string, db *lib.DB) {
	parser.InitializeTextOutput()
	var err error
	var commandInfo = make(map[string]interface{})
	var identifier = slave["username"]+"@"+slave["host"]
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
	var counter = 0
	var outputChannel = make(chan []byte)
	var rawCombinedOutput []string

	go scanOutput(outputChannel, *stdout)

	commandInfo["output"] = config["output"]
	commandInfo["no_of_outputs"] = counter
	commandInfo["rules"] = config["rules"]
	commandInfo["raw_output"] = rawCombinedOutput
	commandInfoBytes, _ := json.Marshal(commandInfo)

	err = db.Write(identifier+"_"+commandInfo["name"].(string), commandInfoBytes)
	if err != nil {
		log.Fatal("Error writing to data.")
		panic(err)
	}
	var deserializedCmdMeta map[string]interface{}
	for row := range outputChannel {
		scanPosition += 1
		if checkIgnored(ignore.([]interface{}), scanPosition) {
			continue;
		}
		rawOutput := string(row)
		rawCombinedOutput = append(rawCombinedOutput, rawOutput)
		rowParser.Populate(&rawOutput)
		counter += 1
		key := identifier + "_" + commandInfo["name"].(string) + "_" + strconv.Itoa(counter)
		err = db.Write(key, rowParser.To_json())
		if err != nil {
			fmt.Println("Could not write to Badger.")
			panic(err)
		}
		cmdMeta, err := db.Read(identifier + "_" + commandInfo["name"].(string))
		if err != nil {
			log.Fatal("Error Reading  data.")
			panic(err)
		}
		_ = json.Unmarshal([]byte(cmdMeta), &deserializedCmdMeta)

		deserializedCmdMeta["no_of_outputs"] = counter
		deserializedCmdMeta["raw_output"] = rawCombinedOutput
		commandInfoBytes, _ = json.Marshal(deserializedCmdMeta)
		err = db.Write(identifier+"_"+commandInfo["name"].(string), commandInfoBytes)
		if err != nil {
			fmt.Println("There is error.")
			panic(err)
		}

	}
	flag <- true
}
