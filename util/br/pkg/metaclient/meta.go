package metaclient

import (
	"fmt"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-clients/go/nebula/meta"
	"go.uber.org/zap"
)

type MetaClient struct {
	client *meta.MetaServiceClient
	log    *zap.Logger
}

var defaultTimeout time.Duration = 120 * time.Second

func NewMetaClient(log *zap.Logger) *MetaClient {
	return &MetaClient{log: log}
}

func (m *MetaClient) RestoreMeta(req *meta.RestoreMetaReq) (*meta.ExecResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.RestoreMeta(req)
}

func (m *MetaClient) CreateBackup(req *meta.CreateBackupReq) (*meta.CreateBackupResp, error) {
	if m.client == nil {
		return nil, fmt.Errorf("client not open")
	}
	return m.client.CreateBackup(req)
}

func (m *MetaClient) Open(addr string) error {

	if m.client != nil {
		if err := m.client.Transport.Close(); err != nil {
			m.log.Warn("close backup falied", zap.Error(err))
		}
	}

	timeoutOption := thrift.SocketTimeout(defaultTimeout)
	addressOption := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return err
	}

	transport := thrift.NewBufferedTransport(sock, 128<<10)

	pf := thrift.NewBinaryProtocolFactoryDefault()
	client := meta.NewMetaServiceClientFactory(transport, pf)
	if err := client.Transport.Open(); err != nil {
		return err
	}
	m.client = client
	return nil
}

func (m *MetaClient) Close() error {
	if m.client != nil {
		if err := m.client.Transport.Close(); err != nil {
			return err
		}
	}
	return nil
}
