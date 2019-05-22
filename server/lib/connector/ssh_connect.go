package cli

import (
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
)

type SSHCONNECTION struct {
	Username string
	Host     string
	Password  string
	Client   *ssh.Client
	Session  *ssh.Session
}

func NewConnection(username, host, password string) SSHCONNECTION {
	sshconn := SSHCONNECTION{}
	sshconn.Username = username
	sshconn.Host = host
	sshconn.Password = password
	sshconn.Client = nil
	sshconn.Session = nil
	return sshconn
}

func (sshconn *SSHCONNECTION) ConnectRemote() error {
	sshConfig := &ssh.ClientConfig{
		User:            sshconn.Username,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.Password(sshconn.Password)},
	}

	client, err := ssh.Dial("tcp", sshconn.Host, sshConfig)
	if err != nil {
		fmt.Println("There is error connecting to the host. ", err)
		return err
	}

	//session, err := client.NewSession()
	//if err != nil {
	//	fmt.Println("Cannot create session.")
	//	session = nil
	//}

	sshconn.Client = client
	sshconn.Session = nil
	return nil
}

func (sshconn *SSHCONNECTION) executeCommand(command string) ([]byte, error) {

	if sshconn.Client == nil {
		return nil, errors.New("No remote Connection")
	} else {
		session, err := sshconn.Client.NewSession()
		if err != nil {
			panic(err)
		}
		res, err := session.CombinedOutput(command)
		defer session.Close()
		if err != nil {
			fmt.Println("Error executing command.")
			return nil, errors.New("Error executing command.")
		}
		return res, err
	}
}

// Using shell complicates things. Stopped For Now.
func (sshconn *SSHCONNECTION) createShell() (io.WriteCloser, io.Reader, error) {
	var err error
	if sshconn.Client == nil || sshconn.Session == nil {
		return nil, nil, errors.New("No remote Connection")
	}


	if err != nil {
		return nil, nil, errors.New("Cannot create shell.")
	}
	stdin, err := sshconn.Session.StdinPipe()
	if err != nil {
		panic(err)
	}
	stdout, _ := sshconn.Session.StdoutPipe()
	err = sshconn.Session.Shell()
	return stdin, stdout, nil

}

func (sshconn *SSHCONNECTION) Close() error {
	return sshconn.Client.Close()
}

func publickeyfile(file string) ssh.AuthMethod {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return nil
	}

	key, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		return nil
	}
	return ssh.PublicKeys(key)
}
