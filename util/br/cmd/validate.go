package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/vesoft-inc/nebula-storage/util/br/pkg/ssh"
	"go.uber.org/zap"
)

func checkSSH(addrs []string, user string, log *zap.Logger) error {
	for _, addr := range addrs {
		ipAddr := strings.Split(addr, ":")
		if len(ipAddr) != 2 {
			return fmt.Errorf("The ip address must contain the port")
		}
		session, err := ssh.NewSshSession(ipAddr[0], user, log)
		if err != nil {
			log.Error("must enable SSH tunneling")
			return err
		}
		session.Close()
	}
	return nil
}

func checkPathAbs(path string) bool {
	return filepath.IsAbs(path)
}
