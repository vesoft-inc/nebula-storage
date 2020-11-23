package restore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/scylladb/go-set/strset"
	"github.com/vesoft-inc/nebula-clients/go/nebula/"
	"github.com/vesoft-inc/nebula-clients/go/nebula/meta"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/config"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/metaclient"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/ssh"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Restore struct {
	config       config.RestoreConfig
	backend      storage.ExternalStorage
	log          *zap.Logger
	client       *metaclient.MetaClient
	metaFileName string
}

type spaceInfo struct {
	spaceID nebula.GraphSpaceID
	cpDir   string
}

type restoreError struct {
	msg string
	Err error
}

var LeaderNotFoundError = errors.New("not found leader")
var restoreFailed = errors.New("restore failed")

func NewRestore(config config.RestoreConfig, log *zap.Logger) *Restore {
	backend, err := storage.NewExternalStorage(config.BackendUrl, log)
	if err != nil {
		log.Error("new external storage failed", zap.Error(err))
		return nil
	}
	backend.SetBackupName(config.BackupName)
	return &Restore{config: config, log: log, backend: backend}
}

func (r *Restore) checkPhysicalTopology(info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) error {
	s := strset.New()
	for _, v := range info {
		for _, c := range v.CpDirs {
			s.Add(hostaddrToString(c.Host))
		}
	}

	if s.Size() != len(r.config.StorageAddrs) {
		return fmt.Errorf("The physical topology of storage must be consistent")
	}
	return nil
}

func (r *Restore) downloadMetaFile() error {
	r.metaFileName = r.config.BackupName + ".meta"
	cmdStr := r.backend.RestoreMetaFileCommand(r.metaFileName, "/tmp/")
	cmd := exec.Command(cmdStr[0], cmdStr[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd.Wait()

	return nil
}

func (r *Restore) restoreMetaFile() (*meta.BackupMeta, error) {
	file, err := os.OpenFile("/tmp/"+r.metaFileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	trans := thrift.NewStreamTransport(file, file)

	binaryIn := thrift.NewBinaryProtocol(trans, false, true)
	defer trans.Close()
	m := meta.NewBackupMeta()
	err = m.Read(binaryIn)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (r *Restore) downloadMeta(g *errgroup.Group, file []string) []string {
	cmd, sstFiles := r.backend.RestoreMetaCommand(file, r.config.MetaDataDir)
	for _, ip := range r.config.MetaAddrs {
		ipAddr := strings.Split(ip, ":")
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr[0], r.config.MetaUser, cmd, r.log) })
	}
	return sstFiles
}

func hostaddrToString(host *nebula.HostAddr) string {
	return host.Host + ":" + strconv.Itoa(int(host.Port))
}

func (r *Restore) downloadStorage(g *errgroup.Group, info map[nebula.GraphSpaceID]*meta.SpaceBackupInfo) map[string]string {
	idMap := make(map[string][]string)
	for gid, bInfo := range info {
		for _, dir := range bInfo.CpDirs {
			idStr := strconv.FormatInt(int64(gid), 10)
			idMap[hostaddrToString(dir.Host)] = append(idMap[hostaddrToString(dir.Host)], idStr)
		}
	}

	i := 0
	storageIPmap := make(map[string]string)
	for ip, ids := range idMap {
		r.log.Info("download", zap.String("ip", ip), zap.String("storage", r.config.StorageAddrs[i]))
		ipAddr := strings.Split(ip, ":")
		cmd := r.backend.RestoreStorageCommand(ipAddr[0], ids, r.config.StorageDataDir+"/nebula")
		addr := strings.Split(r.config.StorageAddrs[i], ":")
		storageIPmap[ip] = r.config.StorageAddrs[i]
		g.Go(func() error { return ssh.ExecCommandBySSH(addr[0], r.config.StorageUser, cmd, r.log) })
		i++
	}

	for _, s := range r.config.StorageAddrs {
		if _, ok := storageIPmap[s]; ok {
			delete(storageIPmap, s)
			continue
		}
	}

	return storageIPmap
}
func stringToHostAddr(host string) (*nebula.HostAddr, error) {
	ipAddr := strings.Split(host, ":")
	port, err := strconv.ParseInt(ipAddr[1], 10, 32)
	if err != nil {
		return nil, err
	}
	return &nebula.HostAddr{ipAddr[0], nebula.Port(port)}, nil
}

func (r *Restore) restoreMeta(sstFiles []string, storageIDMap map[string]string, count int) error {
	r.log.Info("restoreMeta")
	var hostMap []*meta.HostPair
	if len(storageIDMap) != 0 {
		for k, v := range storageIDMap {
			fromAddr, err := stringToHostAddr(k)
			if err != nil {
				return err
			}
			toAddr, err := stringToHostAddr(v)
			if err != nil {
				return err
			}

			pair := &meta.HostPair{fromAddr, toAddr}
			hostMap = append(hostMap, pair)
		}
	}
	for {
		if count == 0 {
			return LeaderNotFoundError
		}
		restoreReq := meta.NewRestoreMetaReq()
		defer r.client.Close()
		restoreReq.Hosts = hostMap
		restoreReq.Files = sstFiles

		resp, err := r.client.RestoreMeta(restoreReq)
		if err != nil {
			return err
		}

		if resp.GetCode() != meta.ErrorCode_E_LEADER_CHANGED && resp.GetCode() != meta.ErrorCode_SUCCEEDED {
			r.log.Error("restore failed", zap.String("error code", resp.GetCode().String()))
			return restoreFailed
		}

		if resp.GetCode() == meta.ErrorCode_SUCCEEDED {
			return nil
		}

		leader := resp.GetLeader()
		if leader == meta.ExecResp_Leader_DEFAULT {
			return LeaderNotFoundError
		}

		// we need reconnect the new leader
		err = r.client.Open(hostaddrToString(leader))
		if err != nil {
			return err
		}
		count--
	}
}

func (r *Restore) cleanupOriginal() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, addr := range r.config.StorageAddrs {
		cmd := r.backend.RestoreStoragePreCommand(r.config.StorageDataDir + "/nebula")
		ipAddr := strings.Split(addr, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, r.config.StorageUser, cmd, r.log) })
	}

	for _, addr := range r.config.MetaAddrs {
		cmd := r.backend.RestoreMetaPreCommand(r.config.MetaDataDir + "/nebula")
		ipAddr := strings.Split(addr, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, r.config.StorageUser, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) stopCluster() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, addr := range r.config.StorageAddrs {
		cmd := "cd " + r.config.StorageRootDir + " && scripts/nebula.service stop storaged"
		ipAddr := strings.Split(addr, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, r.config.StorageUser, cmd, r.log) })
	}

	for _, addr := range r.config.MetaAddrs {
		cmd := "cd " + r.config.MetaRootDir + " && scripts/nebula.service stop metad"
		ipAddr := strings.Split(addr, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, r.config.MetaUser, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) startMetaService() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, addr := range r.config.MetaAddrs {
		cmd := "cd " + r.config.MetaRootDir + " && scripts/nebula.service start metad &>/dev/null &"
		ipAddr := strings.Split(addr, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, r.config.MetaUser, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) startStorageService() error {
	g, _ := errgroup.WithContext(context.Background())
	for _, addr := range r.config.StorageAddrs {
		cmd := "cd " + r.config.StorageRootDir + " && scripts/nebula.service start storaged &>/dev/null &"
		ipAddr := strings.Split(addr, ":")[0]
		g.Go(func() error { return ssh.ExecCommandBySSH(ipAddr, r.config.StorageUser, cmd, r.log) })
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (r *Restore) RestoreCluster() error {

	err := r.downloadMetaFile()
	if err != nil {
		r.log.Error("download meta file failed", zap.Error(err))
		return err
	}

	m, err := r.restoreMetaFile()

	if err != nil {
		r.log.Error("restore meta file failed", zap.Error(err))
		return err
	}

	err = r.checkPhysicalTopology(m.BackupInfo)
	if err != nil {
		r.log.Error("check physical failed", zap.Error(err))
		return err
	}

	err = r.stopCluster()
	if err != nil {
		r.log.Error("stop cluster failed", zap.Error(err))
		return err
	}

	err = r.cleanupOriginal()
	if err != nil {
		r.log.Error("cleanup original failed", zap.Error(err))
		return err
	}

	g, _ := errgroup.WithContext(context.Background())

	sstFiles := r.downloadMeta(g, m.MetaFiles)
	storageIDMap := r.downloadStorage(g, m.BackupInfo)

	err = g.Wait()
	if err != nil {
		r.log.Error("restore error")
		return err
	}

	err = r.startMetaService()
	if err != nil {
		r.log.Error("start cluster failed", zap.Error(err))
		return err
	}

	time.Sleep(time.Second * 3)

	r.client = metaclient.NewMetaClient(r.log)
	err = r.client.Open(r.config.MetaAddrs[0])
	if err != nil {
		r.log.Error("open meta failed", zap.Error(err))
		return err
	}

	err = r.restoreMeta(sstFiles, storageIDMap, 3)
	if err != nil {
		r.log.Error("restore meta file failed", zap.Error(err))
		return err
	}

	err = r.startStorageService()
	if err != nil {
		r.log.Error("start storage service failed", zap.Error(err))
		return err
	}

	r.log.Info("restore meta successed")

	return nil

}
