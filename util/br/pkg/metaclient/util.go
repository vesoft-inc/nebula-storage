package metaclient

import (
	"strconv"

	"github.com/vesoft-inc/nebula-storage/util/br/pkg/nebula"
)

func HostaddrToString(host *nebula.HostAddr) string {
	return host.Host + ":" + strconv.Itoa(int(host.Port))
}
