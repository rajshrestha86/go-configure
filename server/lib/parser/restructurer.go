package parser

import (
	"encoding/json"
	"fmt"
	"github.com/Knetic/govaluate"
	"log"
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
	if match == nil {
		return
	}
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

func (row *TableRow) EvaluateExpressions(rules string) (bool, error) {
	expression, err := govaluate.NewEvaluableExpression(rules)
	result, err := expression.Evaluate(row.Data)
	if err != nil {
		return false, err
	}

	return result.(bool), err
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

func (t *TextOutput) To_bytes() []byte {
	return t.Text
}

func (t *TextOutput) To_string() string {
	return string(t.Text)
}

func TextOutputFromBytes(data []byte) TextOutput {
	return TextOutput{nil, data}
}

func (t *TextOutput) Populate(data []byte) {
	t.Text = data
}

func (t *TextOutput) Evaluate_regex(reg []interface{}) []interface{} {
	var regex = ``
	var result []interface{}
	for _, each := range reg {
		regex = regex + each.(string)
	}
	re, _ := regexp.Compile(regex)
	res := re.FindAll(t.Text, -1)
	for _, each := range res {
		result = append(result, string(each))
	}
	return result
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
