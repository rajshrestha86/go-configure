package main

import (
	"bufio"
	"bytes"
	"cli"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

func main() {

	var cmds []cli.Command
	var reader *bufio.Reader
	var dir string
	var input_cmd []byte
	var err error
	var args []string
	// Infinite loop get command and process it.

	for {
		proc := cli.Command{}
		reader = bufio.NewReader(os.Stdin)
		dir, err = os.Getwd()
		if err != nil {
			panic(err)
		}

		fmt.Print("raracli (" + dir + ")-> ")
		input_cmd, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Exited main loop.")
				break
			} else {
				panic(err)
			}
		}

		proc.Cmd = input_cmd
		in := strings.Split(string(input_cmd), " ")

		if len(in) == 1 {
			args = []string{}
		} else {
			args = in[1:]
		}

		// If the input command is a system command then it will be processed accordingly.
		sys_cmd := cli.IsSysCommand(in[0])
		if sys_cmd {
			fmt.Println("This is a system command.", len(args))
			cli.ExecuteCommand(in[0], args)
			continue
		} else if in[0] == "exit" || in[0] == "killed" {
			break
		} else if in[0] == "exit_routine" {
			fmt.Println("Got exit on external scanner.")
			continue
		}

		cmd := exec.Command(in[0], args...)
		cmd.Dir, _ = os.Getwd()
		cmd.Stdin = os.Stdin
		out, _ := cmd.CombinedOutput()
		scan_output(&out, &proc)
		cmds = append(cmds, proc)
	}

	// Gets the input and output for the given process.
	for _, each := range cmds {
		fmt.Println("Process: ", string(each.Cmd))
		fmt.Println("Inputs: ")
		for _, inp := range each.Inputs {
			fmt.Println(inp)
		}
		fmt.Println("Outputs: ")
		for _, out := range each.Outputs {
			fmt.Println(out)
		}
	}
}

func scan_output(output *[]byte, proc *cli.Command) {
	output_reader := bytes.NewReader(*output)
	output_scanner := bufio.NewScanner(output_reader)
	scan_position := 0
	for output_scanner.Scan() {
		scan_position += 1
		txt := output_scanner.Text()
		proc.Outputs = append(proc.Outputs, txt)
		fmt.Println(txt)
	}
}
