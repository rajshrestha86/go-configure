package main

import (
	"fmt"
	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"io"
	"log"
	"os"
	"os/exec"
	"rara/client/grpc/manager"
	pb "rara/client/grpc/proto"
	"strings"

	_ "google.golang.org/grpc"
)

const (
	address = "localhost:50000"
)

func usage(w io.Writer) {
	io.WriteString(w, "commands:\n")
	io.WriteString(w, completer.Tree("    "))
}

// Function constructor - constructs new function for listing given directory
func clearConsole() {
	fmt.Println("Cleared")
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	cmd.Run()
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("slave",
		readline.PcItem("-new"),
		readline.PcItem("-rm"),
		readline.PcItem("-list"),
		readline.PcItem("-Rd"),
	),
	readline.PcItem("config",
		readline.PcItem("-file"),
		readline.PcItem("-view"),
		readline.PcItem("-list"),
		readline.PcItem("-rm"),
	),
	readline.PcItem("listen",
		readline.PcItem("-file"),
		readline.PcItem("-slave"),
	),
	readline.PcItem("job",
		readline.PcItem("-list"),
		readline.PcItem("stop",
			readline.PcItem("-id"),
			readline.PcItem("-all"),
		),
	),
	readline.PcItem("output",
		readline.PcItem("-id"),
		readline.PcItem("-list"),
		readline.PcItem("-rm",
			readline.PcItem("-id"),
			readline.PcItem("-timestamp"),
			readline.PcItem("-all"), ),
	),
	readline.PcItem("help"),

)

func filterInput(r rune) (rune, bool) {
	switch r {
	// block CtrlZ feature
	case readline.CharCtrlZ:
		return r, false
	}
	return r, true
}

func main() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:          color.HiBlueString("raracli >> "),
		HistoryFile:     "/tmp/readline.tmp",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold:   true,
		FuncFilterInputRune: filterInput,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	//setPasswordCfg := l.GenPasswordConfig()
	//setPasswordCfg.SetListener(func(line []rune, pos int, key rune) (newLine []rune, newPos int, ok bool) {
	//	l.SetPrompt(fmt.Sprintf("Enter password(%v): ", len(line)))
	//	l.Refresh()
	//	return nil, 0, false
	//})

	log.SetOutput(l.Stderr())
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic("Did not connect. ")
	}
	defer conn.Close()

	client := pb.NewCLIClient(conn)
	RPCManager := manager.RPC{Client: client}
	clearConsole()
	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		if line == "clear" {
			clearConsole()
		} else if line == "help" {
			fmt.Println("slave")
			fmt.Println("Args:")
			fmt.Println("\t -list : List slaves on server. ")
			fmt.Println("\t -Rd : Delete all slaves on server.")
			fmt.Println("\t -new [user@host] [password] : Add new slave on server.")
			fmt.Println("\t -rm [user@host] : Remove slave from server.")
			fmt.Println("")
			fmt.Println("config")
			fmt.Println("Args:")
			fmt.Println("\t -file [filepath] [file_name] : Transfer config file and set name. ")
			fmt.Println("\t -view [config_name]: View contains of config file on server.")
			fmt.Println("\t -list : List of config files on server.")
			fmt.Println("\t -rm [config_name] : Remove config file from server.")
			fmt.Println("")
			fmt.Println("listen")
			fmt.Println("Args:")
			fmt.Println("\t -file [machine_list.csv] : Start listening on the slaves from the file for given config.. ")
			fmt.Println("\t -slave [user@host] [config_name]: Start listening on the slave.")
			fmt.Println("")
			fmt.Println("job")
			fmt.Println("Args:")
			fmt.Println("\t -list : List all running jobs on the server. ")
			fmt.Println("\t stop -id [jobId] : Stop running job with given Id.")
			fmt.Println("\t stop -all  : Stop all running jobs on Server.")
			fmt.Println("")
			fmt.Println("output")
			fmt.Println("Args:")
			fmt.Println("\t -list [user@host]: List all output headers for the given slave. ")
			fmt.Println("\t -id [output_head] -t 'raw'|'table' : Get output for the given head with specified type.")
			fmt.Println("\t -rm -id [output_head]  : Remove specific output identified by given head.")
			fmt.Println("\t -rm -timestamp [user@host:timestamp]  : Remove specific outputs for given timestamp.")
			fmt.Println("\t -rm -all [user@host]  : Remove all outputs for given host.")
		}

		stat, err := RPCManager.InvokeRPC(line)
		if !stat {
			if err != nil {
				color.HiRed(err.Error())
			} else {
				if conn.GetState() == connectivity.TransientFailure {
					color.HiRed("Connection with server has transient failure. Server may have stopped/ ", )
				} else if conn.GetState() == connectivity.Shutdown {
					color.HiRed("Connection with server has shutdown. Try reconnecting.", )
				}
			}
		}
	}
}

