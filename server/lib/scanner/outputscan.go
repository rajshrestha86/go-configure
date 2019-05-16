package scanner

import (
	"bufio"
	"io"
)

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
