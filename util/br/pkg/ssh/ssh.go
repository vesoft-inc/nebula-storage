package ssh

import (
	"bytes"
	"io/ioutil"
	"net"
	"os"

	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

func NewSshSession(addr string, user string, log *zap.Logger) (*ssh.Session, error) {
	key, err := ioutil.ReadFile(os.Getenv("HOME") + "/.ssh/id_rsa")
	if err != nil {
		log.Error("unable to read private key", zap.Error(err))
		return nil, err
	}

	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Error("unable to parse private key", zap.Error(err))
		return nil, err
	}
	config := &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
	}

	client, err := ssh.Dial("tcp", net.JoinHostPort(addr, "22"), config)
	if err != nil {
		log.Error("unable to connect host", zap.Error(err), zap.String("host", addr), zap.String("user", user))
		return nil, err
	}

	session, err := client.NewSession()
	if err != nil {
		log.Error("new session failed", zap.Error(err))
		return nil, err
	}

	return session, nil
}

func ExecCommandBySSH(addr string, user string, cmd string, log *zap.Logger) error {
	session, err := NewSshSession(addr, user, log)
	if err != nil {
		return err
	}
	defer session.Close()
	log.Info("ssh will exec", zap.String("addr", addr), zap.String("cmd", cmd), zap.String("user", user))
	var stdoutBuf bytes.Buffer
	session.Stdout = &stdoutBuf

	err = session.Run(cmd)
	if err != nil {
		log.Error("ssh run failed", zap.Error(err))
		return err
	}
	log.Info("Command execution completed", zap.String("result", stdoutBuf.String()))
	return nil
}
