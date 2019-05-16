package parser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Knetic/govaluate"
	"io"
	"log"
	"rara/server/lib"
	"regexp"
	"strconv"
)

var CastMap = make(map[string]func())

// Represents a row output for the process that gives a table Output.
type TableRow struct {
	Regex     *regexp.Regexp
	Data      map[string]interface{}
	DataTypes map[string]string
}

func (row *TableRow) Populate(d *string) {
	match := row.Regex.FindStringSubmatch(*d)
	for i, name := range row.Regex.SubexpNames() {
		if i != 0 && name != "" {
			if row.DataTypes[name] == "int" {
				row.Data[name] = cast_to_int(match[i])
			} else {
				row.Data[name] = match[i]
			}

		}
	}
}

func (row *TableRow) To_json() []byte {
	_jsn, err := json.Marshal(row.Data)
	if err != nil {
		panic(err)
	}
	return _jsn
}

func (row *TableRow) evaluate_expressions(rules *[]interface{}) bool {
	for _, each := range *rules {
		expression, err := govaluate.NewEvaluableExpression(each.(string))
		result, err := expression.Evaluate(row.Data)
		if err != nil {
			log.Fatal(err)
			panic(err)
		}

		if result.(bool) == false {
			return false
		}
	}

	return true
}

func TableRowFromJson(data []byte) TableRow {
	var objmap map[string]interface{}
	err := json.Unmarshal(data, &objmap)
	if err != nil {
		panic(err)
	}

	return TableRow{nil, objmap, nil}
}

func InitializeTableRow(config []interface{}) TableRow {
	var reg = ``
	var fields = make(map[string]interface{})
	var types = make(map[string]string)
	for _, each := range config {
		field := each.(map[string]interface{})
		fieldName := field["field_name"].(string)
		reg = reg + fmt.Sprintf(`(?P<%s>%s)`, field["field_name"], field["regex"])
		reg = reg + `\s*`
		fields[fieldName] = nil
		types[fieldName] = field["type"].(string)

	}
	re, _ := regexp.Compile(reg)
	return TableRow{re, fields, types}
}

type TextOutput struct {
	Regex *regexp.Regexp
	Text  []byte
}

func (t *TextOutput) to_bytes() []byte {
	return t.Text
}

func TextOutputFromBytes(data []byte) TextOutput {
	return TextOutput{nil, data}
}

func (t *TextOutput) Populate(data []byte) {
	t.Text = data
}

func (t *TextOutput) evaluate_regex(reg *[]interface{}) [][]byte {
	var regex = ``
	for _, each := range *reg {
		regex = regex + each.(string)
	}
	re, _ := regexp.Compile(regex)
	res := re.FindAll(t.Text, -1)
	return res
}

func InitializeTextOutput() (TextOutput) {
	return TextOutput{nil, nil}

}

func cast_to_int(value string) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		log.Fatal("Type Conversion error.", value)
		panic(err)
	}
	return n
}

func parseTableOutput(flag chan bool, config map[string]interface{}, stdout *io.Reader, host *string, db *lib.DB) {
	var err error
	var commandInfo = make(map[string]interface{})
	commandInfo["name"] = config["name"].(string)
	tableConfig := config["table_restruct"]
	// Output Parser Based on the table_format.
	outConfig := tableConfig.([]interface{})
	parser := InitiazlizeTableRow(outConfig)

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

	var scanPosition int = 0
	var counter = 0
	var outputChannel = make(chan []byte)
	var rawCombinedOutput []string

	go scanOutput(outputChannel, *stdout)

	commandInfo["output"] = config["output"]
	commandInfo["no_of_outputs"] = counter
	commandInfo["rules"] = config["rules"]
	commandInfo["raw_output"] = rawCombinedOutput
	commandInfoBytes, _ := json.Marshal(commandInfo)

	err = db.Write(*host+"_"+commandInfo["name"].(string), commandInfoBytes)
	if err != nil {
		log.Fatal("Error writing to data.")
		panic(err)
	}
	var unmarshalledCmdMeta map[string]interface{}
	for row := range outputChannel {
		scanPosition += 1
		if checkIgnored(ignore.([]interface{}), scanPosition) {
			continue;
		}
		rawOutput := string(row)
		rawCombinedOutput = append(rawCombinedOutput, rawOutput)
		parser.Populate(&rawOutput)
		counter += 1
		key := *host + "_" + commandInfo["name"].(string) + "_" + strconv.Itoa(counter)
		err = db.Write(key, parser.To_json())
		if err != nil {
			fmt.Println("Could not write to Badger.")
			panic(err)
		}
		cmdMeta, err := db.Read(*host + "_" + commandInfo["name"].(string))
		if err != nil {
			log.Fatal("Error Reading  data.")
			panic(err)
		}
		_ = json.Unmarshal([]byte(cmdMeta), &unmarshalledCmdMeta)

		unmarshalledCmdMeta["no_of_outputs"] = counter
		unmarshalledCmdMeta["raw_output"] = rawCombinedOutput
		commandInfoBytes, _ = json.Marshal(unmarshalledCmdMeta)
		err = db.Write(*host+"_"+commandInfo["name"].(string), commandInfoBytes)
		if err != nil {
			fmt.Println("There is error.")
			panic(err)
		}

	}
	flag <- true
}

func scanOutput(out chan []byte, stdout io.Reader) {
	outputScanner := bufio.NewScanner(stdout)
	scanPosition := 0
	for outputScanner.Scan() {
		scanPosition += 1
		txt := outputScanner.Text()
		out <- []byte(txt)
	}
	close(out)
}
