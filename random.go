package main

import (
	"fmt"
	"github.com/alexflint/go-restructure/regex"
)

func main(){
	re, _ := regex.Compile(`\w+\s*logged in`)
	res := re.FindAll([]byte("User Raj logged in Succesfully. He was doing something suspicious. Anish logged in to the system."), -1)
	for _, each := range res {
		fmt.Println(string(each))
	}
}
